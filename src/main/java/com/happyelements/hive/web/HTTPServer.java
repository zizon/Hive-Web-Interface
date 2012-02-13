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
		protected final String url;
		protected final Authorizer authorizer;

		/**
		 * constructor
		 * @param need_auth
		 * 		flags indicate if it need auth
		 * @param url
		 * 		the that match the request URL
		 */
		public HTTPHandler(Authorizer authorizer, String url) {
			this.authorizer = authorizer;
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
			this(null, url);
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
			if (this.url != null && this.url.equals(target)) {
				this.handle(request, response);
			} else {
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
			}

			// flag it as finished
			Request.getRequest(request).setHandled(true);
		}

		/**
		 * test user if authorized
		 * @param request
		 * 		the request
		 * @return
		 * 		true if user is authorize or no need for authorization
		 */
		protected boolean auth(HttpServletRequest request) {
			if (this.authorizer != null) {
				return authorizer.auth(request);
			} else {
				return true;
			}
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

	/**
	 * the jsonp handler
	 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
	 */
	public static interface JsonPCapable {
		/**
		 * generate the json response string
		 * @param request
		 * 		the http request
		 */
		public void generateJsonResponse(String callback,
				HttpServletRequest request, HttpServletResponse response)
				throws IOException, ServletException;
	}

	private File static_root;
	private String default_url;
	private ConcurrentHashMap<String, HTTPHandler> rest = new ConcurrentHashMap<String, HTTPServer.HTTPHandler>();

	private Map<String, File> cache = new ConcurrentHashMap<String, File>() {
		private static final long serialVersionUID = -233053974881547599L;
		{
			new Timer().scheduleAtFixedRate(new TimerTask() {
				/**
				 * {@inheritDoc}}
				 * @see java.util.TimerTask#run()
				 */
				@Override
				public void run() {
					clear();
				}
			}, 0, 3600000);
		}

		/**
		 * {@inheritDoc}}
		 * @see java.util.concurrent.ConcurrentHashMap#get(java.lang.Object)
		 */
		@Override
		public File get(Object key) {
			if (key == null) {
				return null;
			}

			File old = super.get(key);
			if (old == null
					&& !(old = new File(HTTPServer.this.static_root,
							key.toString())).exists() //
					&& !old.isFile()) {
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
	public HTTPServer(String static_root, int port, String default_url)
			throws IOException {
		super(port);

		// set up default url
		this.default_url = default_url;

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
				response.setCharacterEncoding("UTF-8");
				// access log
				HTTPServer.LOGGER.info("access path:" + target + " user-agent:"
						+ request.getHeader("User-Agent") + " ip:"
						+ request.getRemoteHost());

				// find handler
				HTTPHandler handler = HTTPServer.this.rest.get(target);
				if (handler != null) {
					// got handler,dispatch it
					handler.handle(target, request, response, dispatch);
				} else if (!"GET".equals(request.getMethod())) {
					// not a get method , give up
					response.sendError(HttpServletResponse.SC_BAD_REQUEST,
							"do not supoort http method except for GET");
				} else {
					// try find static files
					// get if modifyed
					long modify = request.getDateHeader("If-Modified-Since");

					// try find a static file
					File file = HTTPServer.this.cache.get(target);

					if (file != null) {
						HTTPServer.LOGGER.debug("file modify:"
								+ file.lastModified() + " if-modified-since:"
								+ modify);
						// client used if modified since,so check modify time
						if (modify != -1
								&& file.lastModified() / 1000 <= modify / 1000) {
							response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
						} else {
							// modified
							mime(target, response);
							response.setStatus(HttpServletResponse.SC_OK);
							response.addDateHeader("Last-Modified",
									file.lastModified());
							IO.copy(new FileInputStream(file),
									response.getOutputStream());
						}
					} else if (!target.endsWith(HTTPServer.this.default_url)) {
						response.sendRedirect(HTTPServer.this.default_url);
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

	/**
	 * attach content type
	 * @param url
	 * 		the target url
	 * @param response
	 * 		the response
	 */
	protected void mime(String url, HttpServletResponse response) {
		if (url == null || response == null) {
			return;
		}

		if (url.endsWith(".js")) {
			response.setContentType("application/javascript");
		} else if (url.endsWith(".css")) {
			response.setContentType("text/css");
		}
	}
}
