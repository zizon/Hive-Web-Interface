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

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * utility to auth user
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class Authorizer {
	private static final Log LOGGER = LogFactory.getLog(Authorizer.class);

	private Map<String, Long> AUTH_CACHE = new ConcurrentHashMap<String, Long>() {
		private static final long serialVersionUID = -5887028771861026254L;
		private long now = System.currentTimeMillis();
		{
			Central.getTimer().schedule(new TimerTask() {
				@Override
				public void run() {
					now = System.currentTimeMillis();
					for (Entry<String, Long> entry : entrySet()) {
						Long that = entry.getValue();
						if (now - that >= 3600000) {
							remove(entry.getKey());
						}
					}
				}
			}, 0, 60000);
		}

		@Override
		public boolean containsKey(Object key) {
			if (key == null || !super.contains(key)) {
				return false;
			} else {
				this.put(key.toString(), this.now);
				return true;
			}
		}
	};

	/**
	 * auth the user
	 * @param request
	 * 		the http request
	 * @return
	 * 		true if auth OK
	 */
	public boolean auth(HttpServletRequest request) {
		String auth = request.getHeader("Authorization");
		if (auth == null) {
			return false;
		} else if (AUTH_CACHE.containsKey(auth)) {
			return true;
		} else {
			boolean authrized = false;
			HttpURLConnection connection = null;
			try {
				connection = (HttpURLConnection) new URL(
						"https://mail.happyelements.com:4431/auth.txt")
						.openConnection();
				connection.addRequestProperty("Authorization", auth);
				connection.connect();
				if (connection.getResponseCode() == HttpServletResponse.SC_OK) {
					AUTH_CACHE.put(auth, System.currentTimeMillis());
					authrized = true;
				}
			} catch (Exception e) {
				Authorizer.LOGGER.error("fail to auth user with authorization:"
						+ auth, e);
				authrized = false;
			} finally {
				if (connection != null) {
					connection.disconnect();
				}
			}

			return authrized;
		}
	}

	/**
	 * extract the user name from http request
	 * @param request
	 * 		the http request 
	 * @return
	 * 		the user name
	 */
	public String extractUser(HttpServletRequest request) {
		if (request == null) {
			return null;
		}

		String user = null;
		try {
			/*
			user = request.getHeader("Authorization").substring(6);
			// trim tailing CRLF
			do {
				switch (user.charAt(user.length() - 1)) {
				case '\r':
				case '\n':
					user = user.substring(0, user.length() - 1);
					continue;
				}
				break;
			} while (true);
			*/
			user = new String(Base64.decode(request.getHeader("Authorization")
					.substring(6))).split(":")[0];
		} catch (Exception e) {
			Authorizer.LOGGER.error("fail to extract user for Authorization:"
					+ request.getHeader("Authorization"), e);
			user = null;
		}

		return user;
	}
}
