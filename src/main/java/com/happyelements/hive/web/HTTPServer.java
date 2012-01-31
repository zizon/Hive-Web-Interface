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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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
import org.mortbay.util.IO;

/**
 * a simple http server
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class HTTPServer extends Server {
	private static final Log LOGGER = LogFactory.getLog(HTTPServer.class);

	/**
	 * handler that handle http request
	 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
	 */
	public static abstract class HTTPHandler extends AbstractHandler {
		private final String url;
		private final boolean need_auth;

		/**
		 * constructor
		 * @param need_auth
		 * 		flags indicate if it need auth
		 * @param url
		 * 		the that match the request URL
		 */
		public HTTPHandler(boolean need_auth, String url) {
			this.need_auth = need_auth;
			if (url != null) {
				this.url = url;
			} else {
				throw new NullPointerException("could not be null");
			}
		}

		/**
		 * constructor,without auth flags on
		 * @param path
		 * 		the that match the request URL		
		 */
		public HTTPHandler(String url) {
			this(false, url);
		}

		/**
		 * {@inheritDoc}}
		 * @see org.mortbay.jetty.Handler#handle(java.lang.String, javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse, int)
		 */
		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
			// check path
			if (this.url.equals(target)) {
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

		/**
		 * handle the request and response
		 * @param request
		 * 		the request
		 * @param response
		 * 		the response
		 * @throws IOException
		 * 		throw when reading or writing request/response fail
		 * @throws ServletException
		 * 		throw when unexpected failing of processing
		 */
		protected abstract void handle(HttpServletRequest request,
				HttpServletResponse response) throws IOException,
				ServletException;
	}

	private File static_root;
	private ConcurrentHashMap<String, HTTPHandler> rest = new ConcurrentHashMap<String, HTTPServer.HTTPHandler>();

	private Map<String, File> cache = new ConcurrentHashMap<String, File>() {
		private static final long serialVersionUID = -233053974881547599L;
		{
			new Timer().scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					clear();
				}
			}, 0, 3600000);
		}

		public File get(Object key) {
			if (key == null) {
				return null;
			}
			File old = super.get(key);
			if (old == null
					&& !(old = new File(static_root, key.toString())).exists()) {
				old = null;
			}
			return old;
		}
	};

	/**
	 * constructor a http server using nio connector
	 * @param port
	 * @throws IOException 
	 */
	public HTTPServer(String static_root, int port) throws IOException {
		super(port);

		// make static root
		this.static_root = new File(static_root);
		if (this.static_root.exists()) {
			if (!this.static_root.isDirectory()) {
				throw new IOException("path:" + static_root
						+ " is not ad directory");
			}
		} else if (!this.static_root.mkdirs()) {
			throw new IOException("fail to create directory:" + static_root);
		}

		// use NIO connector
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
				HTTPServer.LOGGER.info("access path:" + target);

				// find handler
				HTTPHandler handler = HTTPServer.this.rest.get(target);
				if (handler != null) {
					handler.handle(target, request, response, dispatch);
				} else if (!"GET".equals(request.getMethod())) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST,
							"do not supoort http method except for GET");
				} else {
					// get if modifyed
					long modify = request.getDateHeader("If-Modified-Since");

					// try find a static file
					File file = cache.get(target);

					if (file != null) {
						// client used if modifyed since,so check modify time
						if (modify != -1
								&& file.lastModified() / 1000 > modify / 1000) {
							response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
						} else {
							// modified
							response.setStatus(HttpServletResponse.SC_OK);
							IO.copy(new FileInputStream(file),
									response.getOutputStream());
						}
					} else {
						// no content found
						response.sendError(HttpServletResponse.SC_NOT_FOUND);
					}
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
			this.rest.put(handler.url, handler);
		}
		return this;
	}
}
