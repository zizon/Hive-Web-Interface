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
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobPriority;

import com.happyelements.hive.web.HTTPServer.HTTPHandler;
import com.happyelements.hive.web.HadoopClient;
import com.happyelements.hive.web.Token;

/**
 * internal hander for hive job submission
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class InternalSubmit extends HTTPHandler {

	private static final Log LOGGER = LogFactory.getLog(InternalSubmit.class);

	/**
	 * @param authorizer
	 * @param url
	 */
	public InternalSubmit(String url) {
		super(null, url);
	}

	/**
	 * @see com.happyelements.hive.web.HTTPServer.HTTPHandler#handle(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void handle(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		String token = request.getParameter("token");
		String user = request.getParameter("user");
		if (!Token.Secret.match(token)) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
		}

		try {
			for (String query : request.getParameter("query").trim().split(";")) {
				if (!(query = query.trim()).isEmpty()) {
					final HiveConf conf = new HiveConf(HiveConf.class);
					conf.set("hadoop.job.ugi", (user == null ? "metric-hourly"
							: user) + ",hive");
					HadoopClient.asyncSubmitQuery(query, conf, null,
							JobPriority.LOW);
				}
			}
		} catch (Exception e) {
			LOGGER.error("fail to extract query", e);
		}
		response.setStatus(HttpServletResponse.SC_OK);
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: hehive port \"select ...\"");
			return;
		}

		String port = args[0].trim();
		StringBuilder query = new StringBuilder();
		for (int i = 1; i < args.length; i++) {
			query.append(args[i]).append(' ');
		}

		HttpURLConnection connection = null;
		try {
			connection = (HttpURLConnection) new URL("http://127.0.0.1:" + port
					+ "/internal/submit").openConnection();
			connection.setDoOutput(true);
			connection.getOutputStream().write(
					("user=metric-hourly&token=" + Token.Secret.token
							+ "&query=" + URLEncoder.encode(query.toString(),
							"utf8")).getBytes());
			System.out.println("HTTP status:" + connection.getResponseCode());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}
}
