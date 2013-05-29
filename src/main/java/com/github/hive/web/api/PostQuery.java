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
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
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

	protected static final Set<String> ALLOW_TABLES = new TreeSet<>();
	protected static final ConcurrentMap<String, Set<String>> TABLE_PARTITIONS = new ConcurrentHashMap<>();

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

	{
		ALLOW_TABLES.add("data_1000");
		ALLOW_TABLES.add("data_101");
		ALLOW_TABLES.add("data_102");
		ALLOW_TABLES.add("data_103");
	}

	/**
	 * @param path
	 */
	public PostQuery(Authorizer authorizer, String url, String path)
			throws IOException {
		super(authorizer, url, path);
	}

	/**
	 * {@inheritDoc}
	 * 
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

		// pre-pare for setup
		String query_id = MD5.digestLiteral(user + query + System.nanoTime());
		String old_id = null;
		String key = "" + user + query;
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
			conf.set("he.query.string", query);
			try {
				String err = validate(conf, query);
				// give up if error
				if (err != null) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST, err);
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
					+ " query:" + query);

			// async submit
			HadoopClient.asyncSubmitQuery(query, conf,
					this.makeResultFile(user, query_id));
		}

		// send response
		response.setStatus(HttpServletResponse.SC_OK);
		response.setContentType("application/json");
		response.getWriter().append(
				"{\"id\":\"" + query_id + "\",\"message\":\"query submit\"}");

	}

	protected String validate(final HiveConf conf, final String query)
			throws InterruptedException, ExecutionException {
		return Central.getThreadPool().submit(new Callable<String>() {
			@Override
			public String call() throws Exception {
				SessionState.start(new SessionState(conf));
				try {
					ASTNode tree = ParseUtils
							.findRootNonNullToken(new ParseDriver()
									.parse(query));

					// filter non query
					String err = isSimpleQuery(tree);
					if (err != null) {
						return err;
					}

					BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory
							.get(conf, tree);
					analyzer.analyze(tree, new Context(conf));
					analyzer.validate();
				} catch (Exception e) {
					PostQuery.LOGGER.error("fail to parse query", e);
					return "fail to parese query";
				} finally {
					Hive.closeCurrent();
				}
				return null;
			}
		}).get();
	}

	protected boolean exceedMaxQueryPerUser(String user) {
		Map<String, QueryInfo> querys = HadoopClient.getUserQuerys(user);
		if (querys == null) {
			return false;
		}

		return querys.size() > 5;
	}

	protected static String isSimpleQuery(ASTNode tree) {
		if (tree.getType() != HiveLexer.TOK_QUERY) {
			return "not a query";
		}

		// check count
		if (tree.getChildCount() != 2) {
			return "unknow syntax";
		}

		String table = null;
		Set<String> columns = null;
		// find from table token
		for (Node node : tree.getChildren()) {
			ASTNode ast = (ASTNode) node;
			switch (ast.getType()) {
				case HiveLexer.TOK_FROM:
					table = parseFrom(ast);
					break;
				case HiveLexer.TOK_INSERT:
					columns = parseInsert(ast);
					break;
				default:
					return "unsupport syntax";
			}
		}

		if (table == null) {
			return "no table found";
		} else if (!ALLOW_TABLES.contains(table)) {
			return "table not allowed";
		} else if (columns == null) {
			return "deny as it may require scaning too much data";
		}

		return constrainStatisfy(table, columns);
	}

	protected static Set<String> parseInsert(ASTNode tree) {
		ASTNode ast = null;
		for (Node node : tree.getChildren()) {
			ast = (ASTNode) node;
			if (ast.getType() == HiveLexer.TOK_WHERE) {
				break;
			}
			ast = null;
		}

		// no where statement
		if (ast == null) {
			return null;
		}

		return findColumns(ast, new TreeSet<String>());
	}

	protected static String parseFrom(ASTNode tree) {
		if (tree.getChildCount() != 1) {
			return null;
		}

		// check table ref
		ASTNode ast = (ASTNode) tree.getChild(0);
		if (ast.getType() != HiveLexer.TOK_TABREF) {
			return null;
		}

		// check table name
		ast = (ASTNode) ast.getChild(0);
		if (ast.getType() != HiveLexer.TOK_TABNAME) {
			return null;
		}

		// check table name
		return ((ASTNode) ast.getChild(0)).getText();
	}

	protected static Set<String> findColumns(ASTNode tree, Set<String> columns) {
		if (tree.getType() == HiveLexer.TOK_TABLE_OR_COL) {
			columns.add(tree.getChild(0).getText());
			return columns;
		}

		ArrayList<Node> children = tree.getChildren();
		if (children == null) {
			return columns;
		}

		// drip down
		for (Node node : children) {
			columns = findColumns((ASTNode) node, columns);
		}
		return columns;
	}

	protected static Set<String> getTablePartitions(String table) {
		Set<String> partitions = TABLE_PARTITIONS.get(table);
		if (partitions != null) {
			return partitions;
		}

		try {
			partitions = new TreeSet<>();
			for (FieldSchema schema : Hive.get().getTable(table)
					.getPartitionKeys()) {
				partitions.add(schema.getName());
			}

			TABLE_PARTITIONS.putIfAbsent(table, partitions);
		} catch (HiveException e) {
			LOGGER.error("fail to get hive client", e);
		} finally {
			Hive.closeCurrent();
		}

		return partitions;
	}

	protected static String constrainStatisfy(String table, Set<String> columns) {
		String err = null;
		Set<String> partitions = getTablePartitions(table);
		if (partitions == null) {
			return "no partition found for table:" + table;
		}

		for (String partition : partitions) {
			if (!columns.contains(partition)) {
				err = "partition:" + partition + " not set";
				break;
			}
		}

		return err;
	}

	public static void main(String[] args) {
		String real_query = "select count(*) from data_repository  where  appid='titan_fb_prod' or 1=gid group by appid";
		try {
			ASTNode tree = ParseUtils.findRootNonNullToken(new ParseDriver()
					.parse(real_query));
			System.out.println(tree.dump());
			System.out.println(isSimpleQuery(tree));
			/*
			 * System.out.println(tree.dump()); Set<Table> tables = new
			 * TreeSet<>(new Comparator<Table>() {
			 * 
			 * @Override public int compare(Table o1, Table o2) { if
			 * (o1.getDbName() != o2.getDbName()) { return
			 * o1.getDbName().compareTo(o2.getDbName()); } else { return
			 * o1.getTableName().compareTo(o2.getTableName()); } } });
			 * findTableAccess(tree, tables);
			 * 
			 * System.out.println(tables);
			 * System.out.println("-------------------");
			 * findTableCondition(tree, tables);
			 */
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Hive.closeCurrent();
		}
	}
}
