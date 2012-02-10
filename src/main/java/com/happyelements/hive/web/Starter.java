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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import com.happyelements.hive.web.api.GetQueryResult;
import com.happyelements.hive.web.api.GetUserQuerys;
import com.happyelements.hive.web.api.PostKill;
import com.happyelements.hive.web.api.PostQuery;

/**
 * to start a http server
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class Starter {

	private static final Log LOGGER = LogFactory.getLog(Starter.class);

	/**
	 * initialize log
	 * @param log
	 * 		the log path
	 * @throws IOException
	 * 		throw when fail to create logs
	 */
	public static void initializeLogSystem(String log) throws IOException {
		Starter.checkAndCreate(log);
		Logger logger = Logger.getRootLogger();
		logger.setLevel(Level.DEBUG);
		logger.removeAllAppenders();
		if (log != null) {
			RollingFileAppender appender = new RollingFileAppender(
					new PatternLayout("%d [%t] %-5p %c [%x] - %m%n"), new File(
							log, "log.log").getPath());
			appender.setMaxBackupIndex(10);
			appender.setMaxFileSize("100MB");
			logger.addAppender(appender);
		} else {
			logger.addAppender(new ConsoleAppender(new PatternLayout(
					"%d [%t] %-5p %c [%x] - %m%n")));
		}
	}

	/**
	 * check path
	 * @param path
	 * 		the path to check
	 * @throws IOException
	 * 		throw when path is not exist or is not directory
	 */
	public static void checkAndCreate(String path) throws IOException {
		File file = new File(path);
		if (file.exists()) {
			if (!file.isDirectory()) {
				throw new IOException(path + " is not directory");
			}
		} else if (!file.mkdirs()) {
			throw new IOException("fail to create path:" + path);
		}
	}

	public static void main(String[] args) {
		try {
			// check parameter
			if (args.length != 4) {
				System.out
						.println("Usage ${hadoop} jar ${jar} ${static_root} ${log_root} ${port} ${default_url}");
				return;
			}

			// initialize log
			Starter.initializeLogSystem(args[1]);
			Starter.LOGGER.info("initialize log system done");
			Starter.LOGGER.info("starting http server at port:" + args[2]
					+ " logdir:" + args[1] + " staticfiles:" + args[0]
					+ " defualturl:" + args[3]);

			// construct and start server
			Authorizer authorizer = new Authorizer();
			new HTTPServer(args[0], Integer.parseInt(args[2], 10), args[3])
					.add(new PostQuery(authorizer, "/hwi/submitQuery.jsp",
							args[1]))
					.add(new GetQueryResult(authorizer,
							"/hwi/getQueryResult.jsp", args[1]))
					.add(new GetUserQuerys(authorizer, "/hwi/getUserQuerys.jsp"))
					.add(new PostKill(authorizer, "/hwi/kill.jsp")).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
