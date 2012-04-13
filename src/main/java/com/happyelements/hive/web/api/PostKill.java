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
package com.happyelements.hive.web.api;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import com.happyelements.hive.web.HadoopClient;
import com.happyelements.hive.web.HTTPServer.HTTPHandler;
import com.happyelements.hive.web.HadoopClient.QueryInfo;
import com.happyelements.hive.web.authorizer.Authorizer;

/**
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 *
 */
public class PostKill extends HTTPHandler {

	private static final Log LOGGER = LogFactory.getLog(PostKill.class);

	/**
	 * {@inheritDoc}}
	 */
	public PostKill(Authorizer authorizer, String url) {
		super(authorizer, url);
	}

	/**
	 * @see com.happyelements.hive.web.HTTPServer.HTTPHandler#handle(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void handle(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		// check method
		if (!"DELETE".equals(request.getMethod())) {
			response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
					"do not supoort http method except for POST");
			return;
		}

		// check auth
		if (!auth(request)) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
			return;
		}

		String id = request.getParameter("id");
		if (id == null || id.isEmpty()) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		// check user
		String user = authorizer.extractUser(request);
		if (user == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"find no user name");
			return;
		}

		try {
			QueryInfo info = HadoopClient.getQueryInfo(id);
			if (info != null) {
				RunningJob job = HadoopClient
						.getJob(JobID.forName(info.job_id));
				if (job != null && user.equals(info.user)) {
					switch (job.getJobState()) {
					case JobStatus.RUNNING:
					case JobStatus.PREP:
						job.killJob();
						break;
					}
				}
			}
		} catch (Exception e) {
			PostKill.LOGGER.error("killing query:" + id + " fail", e);
		}

		response.setStatus(HttpServletResponse.SC_OK);
	}

}
