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

		public QueryInfo(String user, String query_id, String query,
				String job_id) {
			this.user = user;
			this.query_id = query_id;
			this.query = query;
			this.job_id = job_id;
		}
	}

	private static final org.apache.hadoop.mapred.JobClient CLIENT;
	static {
		try {
			CLIENT = new org.apache.hadoop.mapred.JobClient(new JobConf(
					new HiveConf()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static long now = System.currentTimeMillis();
	{
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				now = System.currentTimeMillis();
				for (Entry<String, QueryInfo> entry : CACHE.entrySet()) {
					QueryInfo info = entry.getValue();
					if (info == null || now - info.access >= 3600000) {
						CACHE.remove(entry.getKey());
					}
				}
			}
		}, 0, 60000);
	}

	private static final Map<String, QueryInfo> CACHE = new ConcurrentHashMap<String, QueryInfo>() {
		private static final long serialVersionUID = -5844685816187712065L;

		@Override
		public QueryInfo put(String key, QueryInfo value) {
			QueryInfo old = super.put(key, value);
			if (value != null) {
				super.put(value.query_id, value);
				super.put(value.job_id, value);
			}

			return old;
		}

		@Override
		public QueryInfo get(Object key) {
			QueryInfo info = super.get(key);
			if (info == null) {
				try {
					String job_id = key.toString();
					JobConf conf = new JobConf(
							JobTracker.getLocalJobFilePath(JobID
									.forName(job_id)));
					info = new QueryInfo(conf.get("he.user.name"),
							conf.get("rest.query.id"), conf
									.get("hive.query.string", "")
									.replace("\n", " ").replace("\r", " ")
									.replace("\"", "'"), job_id);
				} catch (Exception e) {
					LOGGER.error("fail to get query info by job id:" + key, e);
				}
			}

			return info;
		}
	};

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
		return HadoopClient.CACHE.get(id);
	}

	public static RunningJob getJob(JobID id) throws IOException {
		return CLIENT.getJob(id);
	}
}
