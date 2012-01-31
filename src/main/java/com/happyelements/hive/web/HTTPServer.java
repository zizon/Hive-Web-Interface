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
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;

import com.happyelements.hive.web.api.SubmitQuery;

/**
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 *
 */
public class HTTPServer extends Server {
	private static final Log LOGGER = LogFactory.getLog(HTTPServer.class);

	private ConcurrentHashMap<String, HTTPHandler> rest = new ConcurrentHashMap<String, HTTPServer.HTTPHandler>();

	public static abstract class HTTPHandler extends AbstractHandler {
		private final String path;
		private final boolean need_auth;

		public HTTPHandler(boolean need_auth, String path) {
			this.need_auth = need_auth;
			if (path != null) {
				this.path = path;
			} else {
				throw new NullPointerException("could not be null");
			}
		}

		public HTTPHandler(String path) {
			this(false, path);
		}

		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
			// check path
			if (this.path.equals(target)) {
				// do auth if needed
				if (this.need_auth) {
					if (Authorizer.auth(request)) {
						this.handle(request, response);
					}
				} else {
					this.handle(request, response);
				}
			}

			// flag it as finished
			Request.getRequest(request).setHandled(true);
		}

		public abstract void handle(HttpServletRequest request,
				HttpServletResponse response) throws IOException,
				ServletException;
	}

	/**
	 * constructor a http server using nio connector
	 * @param port
	 */
	public HTTPServer(int port) {
		super(port);

		// use nio connector
		SelectChannelConnector connector = new SelectChannelConnector();
		connector.setDelaySelectKeyUpdate(false);
		connector.setUseDirectBuffers(false);
		this.addConnector(new SelectChannelConnector());
		this.setThreadPool(new QueuedThreadPool());

		// add main handler(REST style)
		this.addHandler(new AbstractHandler() {
			@Override
			public void handle(String target, HttpServletRequest request,
					HttpServletResponse response, int dispatch)
					throws IOException, ServletException {
				// access log
				HTTPServer.LOGGER.info("access path:" + target
						+ request.getHeaderNames());

				// find handler
				HTTPHandler handler = HTTPServer.this.rest.get(target);
				if (handler != null) {
					handler.handle(target, request, response, dispatch);
				} else {
					HTTPServer.LOGGER
							.warn("find no handler for path:" + target);
				}

				// flag it as finished
				Request.getRequest(request).setHandled(true);
			}
		});
	}

	/**
	 * add rest handler
	 * @param handler
	 * 		the handler
	 * @return
	 * 		the server that contains the handler
	 */
	public HTTPServer add(HTTPHandler handler) {
		if (handler != null) {
			this.rest.put(handler.path, handler);
		}
		return this;
	}

	public static void main(String[] args) {
		try {
			new HTTPServer(9999)//
					.add(new SubmitQuery("/")).add(null)//
					.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
