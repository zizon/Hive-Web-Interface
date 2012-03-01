/*
 * Copyright (c) 2012, someone All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1.Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. 2.Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. 3.Neither the name of the Happyelements Ltd. nor the
 * names of its contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.happyelements.hive.web;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * wrapper for some hadoop api
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class HadoopClient {

	private static final Log LOGGER = LogFactory.getLog(HadoopClient.class);

	private static final long INVALIDATE_PERIOD = 3600000 * 4;

	/**
	 * query info,include job status and query/user 
	 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
	 */
	public static class QueryInfo {
		public String user;
		public String query_id;
		public String query;
		public String job_id;
		public long access;
		public JobStatus status;

		/**
		 * constructor
		 * @param user
		 * 		the user name
		 * @param query_id
		 * 		the query id
		 * @param query
		 * 		the query string
		 * @param job_id
		 * 		the job id
		 */
		public QueryInfo(String user, String query_id, String query,
				String job_id) {
			this.user = user;
			this.query_id = query_id;
			this.query = query.replace("\n", " ").replace("\r", " ")
					.replace("\"", "'").replace("\t", " ");
			this.job_id = job_id;
		}

		/**
		 * {@inheritDoc}}
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "user:" + this.user + " query_id:" + this.query_id
					+ " job_id:" + this.job_id + " query:" + this.query;
		}
	}

	private static int refresh_request_count = 0;
	private static final JobClient CLIENT;
	private static final ConcurrentHashMap<String, Map<String, QueryInfo>> USER_JOB_CACHE;
	private static final ConcurrentHashMap<String, QueryInfo> JOB_CACHE;
	static {
		try {
			CLIENT = new JobClient(new JobConf(new HiveConf()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		// create user job cache
		JOB_CACHE = new ConcurrentHashMap<String, HadoopClient.QueryInfo>();
		USER_JOB_CACHE = new ConcurrentHashMap<String, Map<String, QueryInfo>>();

		// schedule user cache update
		Central.schedule(new Runnable() {
			@Override
			public void run() {
				if (HadoopClient.refresh_request_count <= 0) {
					return;
				}
				long now = Central.now();
				try {
					for (JobStatus status : HadoopClient.CLIENT.getAllJobs()) {
						// save job id
						String job_id = status.getJobID().toString();
						// update info
						QueryInfo info = HadoopClient.JOB_CACHE.get(job_id);
						if (info == null) {
							JobConf conf = new JobConf(JobTracker
									.getLocalJobFilePath(status.getJobID()));
							String query = conf.get("hive.query.string");
							String query_id = conf.get("rest.query.id");
							String user = conf.get("he.user.name");
							info = new QueryInfo(user == null ? "" : user, //
									query_id == null ? "" : query_id, //
									query == null ? "" : query, //
									job_id);

							info.access = now;
							HadoopClient.JOB_CACHE.putIfAbsent(job_id, info);
						}

						// update status
						info.status = status;

						// tricky way to update user cache,as a none App,user
						// will be empty
						if (!info.user.isEmpty()) {
							// find user cache
							Map<String, QueryInfo> user_infos = HadoopClient.USER_JOB_CACHE
									.get(info.user);
							if (user_infos == null) {
								user_infos = new ConcurrentHashMap<String, HadoopClient.QueryInfo>();
								Map<String, QueryInfo> old = HadoopClient.USER_JOB_CACHE
										.putIfAbsent(info.user, user_infos);
								user_infos = old == null ? user_infos : old;
							}
							// replicate it
							user_infos.put(info.job_id, info);
							user_infos.put(info.query_id, info);
						}
					}

					// reset flag
					HadoopClient.refresh_request_count = 0;
				} catch (IOException e) {
					HadoopClient.LOGGER.error("fail to refresh old job", e);
				}
			}
		}, 1);

		// schedule query info cache clean up
		Central.schedule(new Runnable() {
			@Override
			public void run() {
				long now = Central.now();

				int before_clean_job_cacha = HadoopClient.JOB_CACHE.size();
				int before_clean_user_job_cache = HadoopClient.USER_JOB_CACHE
						.size();

				// clear user jobs
				for (Entry<String, Map<String, QueryInfo>> entry : HadoopClient.USER_JOB_CACHE
						.entrySet()) {
					// optimize cache size
					boolean empty = true;

					// no user cache
					Map<String, QueryInfo> user_querys = entry.getValue();
					if (user_querys == null) {
						HadoopClient.USER_JOB_CACHE.remove(entry.getKey());
						continue;
					}

					// find eviction
					for (Entry<String, QueryInfo> query_info_entry : user_querys
							.entrySet()) {
						empty = false;
						QueryInfo info = query_info_entry.getValue();

						// clean null info
						if (info == null) {
							user_querys.remove(query_info_entry.getKey());
							continue;
						}

						if (now - info.access >= 3600000
								|| (info.status.getStartTime() > 0 && now
										- info.status.getStartTime() >= HadoopClient.INVALIDATE_PERIOD)) {
							// clean expire info from user cache
							user_querys.remove(query_info_entry.getKey());

							// clean from job cache
							HadoopClient.JOB_CACHE.remove(info.job_id);
							HadoopClient.LOGGER
									.info("remove from user job cache:" + info);
						}
					}

					// no entry in this user cache ,remove it
					if (empty) {
						// it *MAY* help GC
						HadoopClient.USER_JOB_CACHE.remove(entry.getKey());
						HadoopClient.LOGGER.info("remove from user job cache:"
								+ entry.getKey());
					}
				}

				HadoopClient.LOGGER.info("job cache:"
						+ HadoopClient.JOB_CACHE.size() + " before:"
						+ before_clean_job_cacha + " user job cache:"
						+ HadoopClient.USER_JOB_CACHE.size() + " before:"
						+ before_clean_user_job_cache);
			}
		}, 60);
	}

	/**
	 * find query info by either job id or query id
	 * @param id
	 * 		either job id or query id
	 * @return
	 * 		the query info
	 */
	public static QueryInfo getQueryInfo(String id) {
		// trigger refresh
		HadoopClient.refresh_request_count++;

		QueryInfo info;
		// lucky case
		if ((info = HadoopClient.JOB_CACHE.get(id)) != null) {
			return info;
		}

		// may be a query id loop it
		for (Entry<String, Map<String, QueryInfo>> entry : HadoopClient.USER_JOB_CACHE
				.entrySet()) {
			// find match
			if ((info = entry.getValue().get(id)) != null) {
				break;
			}
		}
		return info;
	}

	/**
	 * get running job status
	 * @param id
	 * 		the JobID of job
	 * @return
	 * 		the running job if exist
	 * @throws IOException
	 * 		thrown when communicate to jobtracker fail
	 */
	public static RunningJob getJob(JobID id) throws IOException {
		return HadoopClient.CLIENT.getJob(id);
	}

	/**
	 * get running job status
	 * @param id
	 * 		the JobID of job
	 * @return
	 * 		the running job if exist
	 * @throws IOException
	 * 		thrown when communicate to jobtracker fail
	 */
	public static Map<String, QueryInfo> getAllQuerys() throws IOException {
		return HadoopClient.JOB_CACHE;
	}

	/**
	 * get the user query
	 * @param user
	 * 		the user name
	 * @return
	 * 		the users name
	 */
	public static Map<String, QueryInfo> getUserQuerys(String user) {
		// trigger refresh
		HadoopClient.refresh_request_count++;
		return HadoopClient.USER_JOB_CACHE.get(user);
	}

	/**
	/**
	 * async submit a query
	 * @param user
	 * 		the submit user
	 * @param query_id
	 * 		the query id
	 * @param query
	 * 		the query
	 * @param conf
	 * 		the hive conf
	 * @param priority
	 * 		the priority
	 */
	public static void asyncSubmitQuery(final String query,
			final HiveConf conf, final File out_file, final JobPriority priority) {
		Central.getThreadPool().submit(new Runnable() {
			@Override
			public void run() {
				conf.setEnum("mapred.job.priority", priority != null ? priority
						: JobPriority.NORMAL);
				SessionState session = new SessionState(conf);
				session.setIsSilent(true);
				session.setIsVerbose(true);
				SessionState.start(session);
				Driver driver = new Driver();
				driver.init();
				try {
					if (driver.run(query).getResponseCode() == 0
							&& out_file != null) {
						FileOutputStream file = null;
						try {
							ArrayList<String> result = new ArrayList<String>();
							driver.getResults(result);
							JobConf job = new JobConf(conf, ExecDriver.class);
							FetchOperator operator = new FetchOperator(driver
									.getPlan().getFetchTask().getWork(), job);
							String serdeName = HiveConf.getVar(conf,
									HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE);
							Class<? extends SerDe> serdeClass = Class
									.forName(serdeName, true,
											JavaUtils.getClassLoader())
									.asSubclass(SerDe.class);
							// cast only needed for
							// Hadoop
							// 0.17 compatibility
							SerDe serde = ReflectionUtils.newInstance(
									serdeClass, null);
							Properties serdeProp = new Properties();

							// this is the default
							// serialization format
							if (serde instanceof DelimitedJSONSerDe) {
								serdeProp.put(Constants.SERIALIZATION_FORMAT,
										"" + Utilities.tabCode);
								serdeProp.put(
										Constants.SERIALIZATION_NULL_FORMAT,
										driver.getPlan().getFetchTask()
												.getWork()
												.getSerializationNullFormat());
							}
							serde.initialize(job, serdeProp);
							file = new FileOutputStream(out_file);
							InspectableObject io = operator.getNextRow();
							while (io != null) {
								file.write((((Text) serde
										.serialize(io.o, io.oi)).toString() + "\n")
										.getBytes());
								io = operator.getNextRow();
							}
						} catch (Exception e) {
							HadoopClient.LOGGER
									.error("unexpected exception when writing result to files",
											e);
						} finally {
							try {
								if (file != null) {
									file.close();
								}
							} catch (IOException e) {
								HadoopClient.LOGGER.error("fail to close file:"
										+ file, e);
							}
						}
					}
				} catch (Exception e) {
					HadoopClient.LOGGER.error("fail to submit querys", e);
				} finally {
					driver.close();
				}
			}
		});
	}

	/**
	 * async submit a query
	 * @param user
	 * 		the submit user
	 * @param query_id
	 * 		the query id
	 * @param query
	 * 		the query
	 * @param conf
	 * 		the hive conf
	 */
	public static void asyncSubmitQuery(final String query,
			final HiveConf conf, final File out_file) {
		HadoopClient.asyncSubmitQuery(query, conf, out_file, JobPriority.HIGH);
	}
}
