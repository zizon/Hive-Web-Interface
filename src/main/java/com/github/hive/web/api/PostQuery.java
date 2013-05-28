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
package com.github.hive.web.api;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

import com.github.hive.web.Central;
import com.github.hive.web.HadoopClient;
import com.github.hive.web.HadoopClient.QueryInfo;
import com.github.hive.web.MD5;
import com.github.hive.web.authorizer.Authorizer;

/**
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class PostQuery extends ResultFileHandler {

	private static final Log LOGGER = LogFactory.getLog(PostQuery.class);
	private static final Set<String> ALLOW_TABLES = new TreeSet<>();
	{
	}

	private ConcurrentMap<String, String> querys = new ConcurrentHashMap<String, String>() {
		private static final long serialVersionUID = 1506831529920830173L;
		{
			Central.schedule(new Runnable() {
				@Override
				public void run() {
					clear();
				}
			}, 60 * 30);
		}
	};

	/**
	 * @param path
	 */
	public PostQuery(Authorizer authorizer, String url, String path)
			throws IOException {
		super(authorizer, url, path);
	}

	/**
	 * @see com.github.hive.web.HTTPServer.HTTPHandler#handle(javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
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

		String ds_start = null;
		String ds_end = null;
		String appid = null;
		{
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			ds_start = request.getParameter("ds_start");
			ds_end = request.getParameter("ds_end");
			appid = request.getParameter("appid");
			if (ds_start == null || ds_end == null || appid == null) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST,
						"ds_start not specified");
				return;
			}

			try {
				long start = format.parse(ds_start).getTime();
				long end = format.parse(ds_end).getTime();

				long diff = end - start;
				if (diff < 0) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST,
							"ds range mis-arrange");
					return;
				} else if (diff >= 30 * 24 * 60 * 60 * 1000L) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST,
							"ds range too broad");
					return;
				}
			} catch (Exception e) {
				LOGGER.error("can not parse ds_start:" + ds_start);
				response.sendError(HttpServletResponse.SC_BAD_REQUEST,
						"ds range not match");
				return;
			}
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

		// limit user query
		if (exceedMaxQueryPerUser(user)) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "user:"
					+ user + " exceed querys limit");
			return;
		}

		// check query
		final String query;
		try {
			query = request.getParameter("query").split(";")[0].trim();
		} catch (Exception e) {
			PostQuery.LOGGER.error("fail to extract query", e);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"no query string found");
			return;
		}

		String deny_reason = null;
		if (query == null || query.isEmpty()
				|| !query.trim().toLowerCase().startsWith("select")
				|| query.toLowerCase().contains("drop ")) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"no query string found");
			return;
		} else if ((deny_reason = tableAllow(query)) != null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "table "
					+ deny_reason + " access not allowed");
			return;
		}

		// enhance query
		final String real_query = enhanceQuery(query, ds_start, ds_end, appid);

		// pre-pare for setup
		String query_id = MD5.digestLiteral(user + real_query
				+ System.nanoTime());
		String old_id = null;
		String key = "" + user + real_query;
		if (request.getParameter("force") == null
				&& (old_id = querys.putIfAbsent(key, query_id)) != null) {
			query_id = old_id;
		} else {
			// submit querys
			querys.put(key, query_id);

			// set up hive
			final HiveConf conf = new HiveConf(HiveConf.class);
			conf.set("hadoop.job.ugi", user + ",hive");
			conf.set("he.user.name", user);
			conf.set("rest.query.id", query_id);
			conf.set("he.query.string", real_query);
			try {
				Boolean parsed = Central.getThreadPool()
						.submit(new Callable<Boolean>() {
							@Override
							public Boolean call() throws Exception {
								SessionState.start(new SessionState(conf));
								try {
									ASTNode tree = ParseUtils
											.findRootNonNullToken(new ParseDriver()
													.parse(real_query));
									BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory
											.get(conf, tree);
									analyzer.analyze(tree, new Context(conf));
									analyzer.validate();
								} catch (Exception e) {
									PostQuery.LOGGER.error(
											"fail to parse query", e);
									return false;
								}
								return true;
							}
						}).get();
				if (parsed == null || !parsed) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST,
							"parse query fail");
					return;
				}
			} catch (Exception e) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST,
						"submit query fail");
				PostQuery.LOGGER.error("fail to parse query", e);
				return;
			}

			// log query submit
			PostQuery.LOGGER.info("user:" + user + " submit:" + query_id
					+ " query:" + real_query);

			// async submit
			HadoopClient.asyncSubmitQuery(real_query, conf,
					this.makeResultFile(user, query_id));
		}

		// send response
		response.setStatus(HttpServletResponse.SC_OK);
		response.setContentType("application/json");
		response.getWriter().append(
				"{\"id\":\"" + query_id + "\",\"message\":\"query submit\"}");
	}

	protected boolean exceedMaxQueryPerUser(String user) {
		Map<String, QueryInfo> querys = HadoopClient.getUserQuerys(user);
		if (querys == null) {
			return false;
		}

		return querys.size() > 5;
	}

	protected String tableAllow(String query) {
		String test = query.toLowerCase();
		Set<String> hive_tables = HadoopClient.hiveTables();
		for (String token : test.split(" ")) {
			for (String minor_token : token.split(".")) {
				if (hive_tables.contains(minor_token)
						&& !ALLOW_TABLES.contains(minor_token)) {
					// seems like not a allowed table
					return minor_token;
				}
			}
		}
		return null;
	}

	protected String enhanceQuery(String query, String ds_start, String ds_end,
			String appid) {
		StringBuilder new_query = new StringBuilder(query.split(";")[0].trim());
		new_query.append(" and (ds>=\"").append(ds_start)
				.append("\" and ds<=\"").append(ds_end)
				.append("\" and appid=\"").append(appid).append("\")");
		return new_query.toString();
	}
}
