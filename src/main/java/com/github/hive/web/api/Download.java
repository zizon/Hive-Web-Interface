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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.util.IO;

/**
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 *
 */
public class Download extends ResultFileHandler {

	/**
	 * @param authorizer
	 * @param url
	 * @param path
	 * @throws IOException
	 */
	public Download(String url, String path) throws IOException {
		super(null, url, path);
	}

	/**
	 * @see com.github.hive.web.HTTPServer.HTTPHandler#handle(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void handle(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		// check method
		if (!"GET".equals(request.getMethod())) {
			response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
					"do not supoort http method except for GET");
			return;
		}

		// check auth
		if (!auth(request)) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
			return;
		}

		// check qeury id
		String query_id = request.getParameter("id");
		if (query_id == null || query_id.isEmpty()) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"find no query");
			return;
		}

		// check user
		String user = request.getParameter("user");
		if (user == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"find no user");
			return;
		}

		File file = makeResultFile(user, query_id);
		if (!file.exists()) {
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		long modify = request.getDateHeader("If-Modified-Since");
		if (modify != -1 && file.lastModified() / 1000 <= modify / 1000) {
			response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
		} else {
			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType("application/octet-stream");
			response.addDateHeader("Last-Modified", file.lastModified());
			IO.copy(new BufferedReader(new FileReader(file)),
					response.getWriter());
		}
		return;
	}

}
