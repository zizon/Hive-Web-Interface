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

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.util.ReflectionUtils;

import com.happyelements.hive.web.Authorizer;
import com.happyelements.hive.web.Central;
import com.happyelements.hive.web.MD5;

/**
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class PostQuery extends ResultFileHandler {

	private static final Log LOGGER = LogFactory.getLog(PostQuery.class);

	/**
	 * @param path
	 */
	public PostQuery(Authorizer authorizer,String url, String path) throws IOException {
		super(authorizer, url, path);
	}

	/**
	 * @see com.happyelements.hive.web.HTTPServer.HTTPHandler#handle(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void handle(HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		// check method
		if (!"POST".equals(request.getMethod())) {
			response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
					"do not supoort http method except for POST");
			return;
		}

		// check auth
		if (!auth(request)) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
			return;
		}
		
		// set up standard responses header
		response.setContentType("application/json");
		
		// check user
		String user = authorizer.extractUser(request);
		if (user == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"find no user name");
			return;
		}

		// check query
		String query = null;
		try {
			query = request.getParameter("query").trim().split(";")[0].trim();
		} catch (Exception e) {
			PostQuery.LOGGER.error("fail to extract query", e);
		}
		if (query == null || query.isEmpty()) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"no query string found");
			return;
		}

		// submit querys
		String query_id = MD5.digestLiteral(user + query
				+ System.currentTimeMillis());
		// set up hive
		final HiveConf conf = new HiveConf(HiveConf.class);
		conf.set("hadoop.job.ugi", user + ",hive");
		conf.set("he.user.name", user);
		conf.set("rest.query.id", query_id);

		SessionState.start(new SessionState(conf));
		try {
			ASTNode tree = ParseUtils.findRootNonNullToken(new ParseDriver()
					.parse(query));
			BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory.get(conf,
					tree);
			analyzer.analyze(tree, new Context(conf));
			analyzer.validate();
		} catch (Exception e) {
			response.setStatus(HttpServletResponse.SC_OK);
			response.setHeader("Content-Type", "application/json");
			response.getWriter().append(
					new StringBuilder("{\"id\":\""
							+ query_id
							+ "\",\"message\":\""
							+ (e.getMessage() != null ? e.getMessage()
									.replace("\"", "'").replace("\n", "") : "")
							+ "\"}").toString());
			return;
		}

		// log query submit
		PostQuery.LOGGER.info("user:" + user + " submit query:" + query);

		// async submit
		this.asyncSubmitQuery(user, query_id, query, conf);

		// send response
		response.setStatus(HttpServletResponse.SC_OK);
		response.setContentType("application/json");
		response.getWriter().append(
				"{\"id\":\"" + query_id + "\",\"message\":\"query submit\"}");
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
	protected void asyncSubmitQuery(final String user, final String query_id,
			final String query, final HiveConf conf) {
		Central.getThreadPool().submit(new Runnable() {
			@Override
			public void run() {
				conf.setEnum("mapred.job.priority", JobPriority.HIGH);
				SessionState.start(new SessionState(conf));
				Driver driver = new Driver();
				driver.init();
				try {
					if (driver.run(query).getResponseCode() == 0) {
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
							file = new FileOutputStream(makeResultFile(user,
									query_id));
							InspectableObject io = operator.getNextRow();
							while (io != null) {
								file.write((((Text) serde
										.serialize(io.o, io.oi)).toString() + "\n")
										.getBytes());
								io = operator.getNextRow();
							}
						} catch (Exception e) {
							PostQuery.LOGGER
									.error("unexpected exception when writing result to files",
											e);
						} finally {
							try {
								if (file != null) {
									file.close();
								}
							} catch (IOException e) {
								PostQuery.LOGGER.error("fail to close file:"
										+ file, e);
							}
						}
					}
				} catch (Exception e) {
					PostQuery.LOGGER.error("fail to submit querys", e);
				} finally {
					driver.close();
				}
			}
		});
	}
}
