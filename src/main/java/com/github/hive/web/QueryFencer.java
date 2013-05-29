/*
 * Copyright (c) <2013>, <Zizon Qiu zzdtsv@gmail.com> All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer. 2. Redistributions in
 * binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution. 3. All advertising materials
 * mentioning features or use of this software must display the following
 * acknowledgement: This product includes software developed by the
 * <organization>. 4. Neither the name of the <organization> nor the names of
 * its contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.hive.web;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

/**
 * 
 */
public class QueryFencer {
	private static final Log LOGGER = LogFactory.getLog(QueryFencer.class);

	protected ConcurrentMap<String, Set<String>> table_partitions = new ConcurrentHashMap<>();

	protected Set<String> allow_tables = new TreeSet<>();

	public QueryFencer(File file) {
		Runnable reload = makeReloadRunnable(file);
		reload.run();
		Central.schedule(reload, 60);
	}

	public String isSimpleQuery(ASTNode tree) {
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
		} else if (!allow_tables.contains(table)) {
			LOGGER.warn("table:" + table + " not allowed");
			return "table not allowed";
		} else if (columns == null) {
			return "deny as it may require scaning too much data";
		}

		return constrainStatisfy(table, columns);
	}

	protected QueryFencer() {
	}

	protected Runnable makeReloadRunnable(final File file) {
		return new Runnable() {
			@Override
			public void run() {
				// load new config
				Properties properties = new Properties();
				try {
					properties.load(new FileInputStream(file));
				} catch (Exception e) {
					LOGGER.error("fail to load white list:" + file, e);
				}

				// get the iterator
				Iterator<Entry<Object, Object>> iterator = properties
						.entrySet().iterator();

				// copy the property
				Set<String> updated_tables = new TreeSet<>();
				while (iterator.hasNext()) {
					updated_tables.add(iterator.next().getKey().toString());
				}

				// replace
				allow_tables = updated_tables;
			}
		};
	}

	protected Set<String> parseInsert(ASTNode tree) {
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

	protected String parseFrom(ASTNode tree) {
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

		StringBuilder builder = new StringBuilder();
		for (Node node : ast.getChildren()) {
			builder.append(((ASTNode) node).getText()).append('.');
		}

		// trim tail
		if (builder.charAt(builder.length() - 1) == '.') {
			builder.setLength(builder.length() - 1);
		}

		// check table name
		return builder.toString();
	}

	protected Set<String> findColumns(ASTNode tree, Set<String> columns) {
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

	protected Set<String> getTablePartitions(String table) {
		Set<String> partitions = table_partitions.get(table);
		if (partitions != null) {
			return partitions;
		}

		try {
			partitions = new TreeSet<>();
			for (FieldSchema schema : Hive.get().getTable(table)
					.getPartitionKeys()) {
				partitions.add(schema.getName());
			}

			table_partitions.putIfAbsent(table, partitions);
		} catch (HiveException e) {
			LOGGER.error("fail to get hive client", e);
		} finally {
			Hive.closeCurrent();
		}

		return partitions;
	}

	protected String constrainStatisfy(String table, Set<String> columns) {
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
			System.out.println(new QueryFencer().isSimpleQuery(tree));
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
