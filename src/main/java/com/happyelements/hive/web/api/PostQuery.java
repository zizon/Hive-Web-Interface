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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import com.happyelements.hive.web.Authorizer;
import com.happyelements.hive.web.HadoopClient;
import com.happyelements.hive.web.MD5;

/**
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class PostQuery extends ResultFileHandler {

	private static final Log LOGGER = LogFactory.getLog(PostQuery.class);

	/**
	 * @param path
	 */
	public PostQuery(Authorizer authorizer, String url, String path)
			throws IOException {
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
		if (!this.auth(request)) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
			return;
		}

		// set up standard responses header
		response.setContentType("application/json");

		// check user
		String user = this.authorizer.extractUser(request);
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
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					e.getMessage());
			return;
		}

		// log query submit
		PostQuery.LOGGER.info("user:" + user + " submit:" + query_id
				+ " query:" + query);

		// async submit
		HadoopClient.asyncSubmitQuery(user, query_id, query, conf,
				this.makeResultFile(user, query_id));

		// send response
		response.setStatus(HttpServletResponse.SC_OK);
		response.setContentType("application/json");
		response.getWriter().append(
				"{\"id\":\"" + query_id + "\",\"message\":\"query submit\"}");
	}

}
