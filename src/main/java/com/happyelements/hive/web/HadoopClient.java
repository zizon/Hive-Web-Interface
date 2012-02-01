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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;

/**
 * wrapper for some hadoop api
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class HadoopClient {

	private static final Log LOGGER = LogFactory.getLog(HadoopClient.class);

	public static class QueryInfo {
		public final String user;
		public final String query_id;
		public final String query;
		public final String job_id;
		public long access;
		public JobStatus status;

		public QueryInfo(String user, String query_id, String query,
				String job_id) {
			this.user = user;
			this.query_id = query_id;
			this.query = query;
			this.job_id = job_id;
		}
	}

	private static int refresh_request_count = 0;
	private static long now = System.currentTimeMillis();
	private static final org.apache.hadoop.mapred.JobClient CLIENT;
	private static final ConcurrentHashMap<String, Map<String, QueryInfo>> USER_JOB_CACHE;
	static {
		try {
			CLIENT = new org.apache.hadoop.mapred.JobClient(new JobConf(
					new HiveConf()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		// create user job cache
		USER_JOB_CACHE = new ConcurrentHashMap<String, Map<String, QueryInfo>>();

		Timer timer = new Timer();
		// schedule user cache update
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				HadoopClient.now = System.currentTimeMillis();
				if (HadoopClient.refresh_request_count <= 0) {
					return;
				}
				try {
					for (JobStatus status : HadoopClient.CLIENT.getAllJobs()) {
						// save job id
						String job_id = status.getJobID().getJtIdentifier();

						// find user querys
						Map<String, QueryInfo> user_infos = HadoopClient.USER_JOB_CACHE
								.get(status.getUsername());
						if (user_infos == null) {
							user_infos = new ConcurrentSkipListMap<String, QueryInfo>();
							HadoopClient.USER_JOB_CACHE
									.put(status.getUsername(), user_infos);
						}

						// update info
						QueryInfo info = user_infos.get(job_id);
						if (info == null) {
							JobConf conf = new JobConf(JobTracker
									.getLocalJobFilePath(status.getJobID()));
							String query = conf.get("hive.query.string");
							String query_id = conf.get("rest.query.id");
							if (query != null) {
								info = new QueryInfo(conf.get("he.user.name"),
										query_id, query.replace("\n", " ")
												.replace("\r", " ")
												.replace("\"", "'"), job_id);
								info.status = status;
								info.access = HadoopClient.now;
								user_infos.put(job_id, info);
								user_infos.put(query_id, info);
							}
						} else {
							info.status = status;
						}
					}
					
					// reset flag
					HadoopClient.refresh_request_count = 0;
				} catch (IOException e) {
					HadoopClient.LOGGER.error("fail to refresh old job", e);
				}
			}
		}, 0, 1000);

		// schedule query info cache clean up
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				HadoopClient.now = System.currentTimeMillis();
				for (Entry<String, Map<String, QueryInfo>> entry : HadoopClient.USER_JOB_CACHE
						.entrySet()) {
					// optimize cache size
					boolean empty = true;

					// find eviction
					Map<String, QueryInfo> user_querys = entry.getValue();
					for (Entry<String, QueryInfo> query_info_entry : user_querys
							.entrySet()) {
						empty = false;
						QueryInfo info = query_info_entry.getValue();
						if (info == null
								|| HadoopClient.now - info.access >= 3600000) {
							user_querys.remove(entry.getKey());
						}
					}

					// no entry in map ,remove it
					if (empty) {
						HadoopClient.USER_JOB_CACHE.remove(entry.getKey());
					}
				}
			}
		}, 0, 60000);

	}

	/**
	 * get all job infos
	 * @return
	 * 		all jobs
	 * @throws IOException
	 * 		unexpected exception when doing RPC
	 */
	public static JobStatus[] getAllJobs() throws IOException {
		return HadoopClient.CLIENT.getAllJobs();
	}

	public static List<JobStatus> jobsToComplete() throws IOException {
		return Arrays.asList(HadoopClient.CLIENT.jobsToComplete());
	}

	/**
	 * find query info by either job id or query id
	 * @param id
	 * 		either job id or query id
	 * @return
	 * 		the query info
	 */
	public static QueryInfo getQueryInfo(String id) {
		QueryInfo info = null;
		for (Entry<String, Map<String, QueryInfo>> entry : HadoopClient.USER_JOB_CACHE
				.entrySet()) {
			// find match
			if ((info = entry.getValue().get(id)) != null) {
				break;
			}
		}
		return info;
	}

	public static RunningJob getJob(JobID id) throws IOException {
		return HadoopClient.CLIENT.getJob(id);
	}

	public static Map<String, QueryInfo> getUserQuerys(String user) {
		return HadoopClient.USER_JOB_CACHE.get(user);
	}
}
