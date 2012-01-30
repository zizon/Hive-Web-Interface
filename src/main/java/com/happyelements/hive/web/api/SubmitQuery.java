/*
Copyright (c) 2012, someone
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions 
are met:

1.Redistributions of source code must retain the above copyright 
notice, this list of conditions and the following disclaimer.
2.Redistributions in binary form must reproduce the above copyright 
notice, this list of conditions and the following disclaimer in the 
documentation and/or other materials provided with the distribution.
3.Neither the name of the Happyelements Ltd. nor the names of its 
contributors may be used to endorse or promote products derived from 
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
POSSIBILITY OF SUCH DAMAGE.
*/
package com.happyelements.hive.web.api;

import java.io.FileOutputStream;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.happyelements.hive.web.ArrayList;
import com.happyelements.hive.web.Base64;
import com.happyelements.hive.web.Date;
import com.happyelements.hive.web.DelimitedJSONSerDe;
import com.happyelements.hive.web.Driver;
import com.happyelements.hive.web.ExecDriver;
import com.happyelements.hive.web.FetchOperator;
import com.happyelements.hive.web.FileLock;
import com.happyelements.hive.web.InspectableObject;
import com.happyelements.hive.web.JobConf;
import com.happyelements.hive.web.MD5;
import com.happyelements.hive.web.Properties;
import com.happyelements.hive.web.SerDe;
import com.happyelements.hive.web.SimpleDateFormat;
import com.happyelements.hive.web.Text;
import com.happyelements.hive.web.HTTPServer.HTTPHandler;

/**
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 *
 */
public class SubmitQuery extends HTTPHandler{

	/**
	 * @param path
	 */
	public SubmitQuery() {
		super(false, "/hwi/submitQuery.jsp");
	}

	/**
	 * @see com.happyelements.hive.web.HTTPServer.HTTPHandler#handle(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void handle(HttpServletRequest request, HttpServletResponse response)throws IOException,
	ServletException {
		// check user
		final String user = new String(Base64
				.decode(request.getHeader("Authorization")
						.replace("Basic", "").trim()));
		if (user == null) {
			response.sendError(
					HttpServletResponse.SC_BAD_REQUEST,
					"no user bind");
			return;
		}
		
		// check query
		final String querys[] = request
				.getParameter("query").trim().split(";");
		if (querys == null || querys.length < 1) {
				response.sendError(
						HttpServletResponse.SC_BAD_REQUEST,
						"no query string found");
			return;
		}


		// set up hive
		 HiveConf conf = new HiveConf(
				HiveConf.class);
		SessionState.start(new SessionState(conf));
		// submit querys
		for (String query : querys) {
			final String id = MD5.digestLiteral(user
					+ query + System.currentTimeMillis());

			try {
				ASTNode tree = ParseUtils
						.findRootNonNullToken(new ParseDriver()
								.parse(query));
				BaseSemanticAnalyzer analyzer = SemanticAnalyzerFactory
						.get(conf, tree);
				analyzer.analyze(tree, new Context(conf));
				analyzer.validate();
			} catch (Exception e) {
				response.setStatus(HttpServletResponse.SC_OK);
				response.setHeader("Content-Type",
						"application/json");
				response.getWriter()
						.append(new StringBuilder(
								"{\"id\":\""
										+ id
										+ "\",\"message\":\""
										+ (e.getMessage() != null ? e
												.getMessage()
												.replace(
														"\"",
														"'")
												.replace(
														"\n",
														"")
												: "")
										+ "\"}").toString());
				return;
			}

			FileOutputStream query_log = null;
			FileLock lock = null;
			try {
				query_log = new FileOutputStream(
						"user.query.log", true);
				String log = "user:"
						+ user
						+ "\t"
						+ "query:"
						+ query
						+ " submit:"
						+ new SimpleDateFormat(
								"yyyy-MM-dd HH-mm-ss")
								.format(new Date()) + "\n";
				lock = query_log.getChannel().lock();
				query_log.write(log.getBytes());
			} catch (Exception e) {
			} finally {
				try {
					if (query_log != null) {
						if (lock != null) {
							lock.release();
						}
						query_log.close();
					}
				} catch (Exception e) {
				}
			}

			new Thread(new Runnable() {
				public void run() {
					conf.set("hadoop.job.ugi", user
							+ ",hive");
					conf.set("he.user.name", user);
					conf.set("rest.query.id", id);
					SessionState.start(new SessionState(
							conf));

					Driver driver = new Driver();
					driver.init();
					try {
						if (driver.run(query)
								.getResponseCode() == 0) {
							FileOutputStream file = null;
							try {
								ArrayList<String> result = new ArrayList<String>();
								driver.getResults(result);
								JobConf job = new JobConf(
										conf,
										ExecDriver.class);
								job.setJobPriority(JobPriority.HIGH);
								FetchOperator operator = new FetchOperator(
										driver.getPlan()
												.getFetchTask()
												.getWork(),
										job);
								String serdeName = HiveConf
										.getVar(conf,
												HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE);
								Class<? extends SerDe> serdeClass = Class
										.forName(
												serdeName,
												true,
												JavaUtils
														.getClassLoader())
										.asSubclass(
												SerDe.class);
								// cast only needed for
								// Hadoop
								// 0.17 compatibility
								SerDe serde = (SerDe) ReflectionUtils
										.newInstance(
												serdeClass,
												null);
								Properties serdeProp = new Properties();

								// this is the default
								// serialization format
								if (serde instanceof DelimitedJSONSerDe) {
									serdeProp
											.put(Constants.SERIALIZATION_FORMAT,
													""
															+ Utilities.tabCode);
									serdeProp
											.put(Constants.SERIALIZATION_NULL_FORMAT,
													driver.getPlan()
															.getFetchTask()
															.getWork()
															.getSerializationNullFormat());
								}
								serde.initialize(job,
										serdeProp);

								file = new FileOutputStream(
										user + "." + id
												+ ".result");
								InspectableObject io = operator
										.getNextRow();
								while (io != null) {
									file.write((((Text) serde
											.serialize(
													io.o,
													io.oi))
											.toString() + "\n")
											.getBytes());
									io = operator
											.getNextRow();
								}
							} catch (Exception e) {
								e.printStackTrace();
							} finally {
								try {
									if (file != null) {
										file.close();
									}
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						driver.close();
					}
				}
			}).start();

			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter()
					.append("{\"id\":\""
							+ id
							+ "\",\"message\":\"query submit\"}");
	}
}
