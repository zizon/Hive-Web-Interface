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
package com.github.hive.web.authorizer;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.hive.web.Central;

/**
 * white list authorizer
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class WhiteListAuthorizer extends Authorizer {

	private static final Log LOGGER = LogFactory
			.getLog(WhiteListAuthorizer.class);

	private Map<String, Boolean> allow_users;

	/**
	 * attach the white list
	 * @param list
	 * 		the white list
	 */
	public WhiteListAuthorizer(File list) {
		// the user list
		this.allow_users = new ConcurrentHashMap<String, Boolean>();

		// reload the user list
		Runnable reload = this.makeReloadWhiteList(list);
		reload.run();
		Central.schedule(reload, 60);
	}

	/**
	 * {@inheritDoc}}
	 * @see com.github.hive.web.authorizer.Authorizer#auth(javax.servlet.http.HttpServletRequest)
	 */
	public boolean auth(HttpServletRequest request) {
		String user = extractUser(request);
		if (user != null && allow_users.containsKey(user)) {
			return super.auth(request);
		}

		return false;
	}

	/**
	 * make reload white list handler
	 * @param list
	 * 		the white list
	 * @return
	 * 		the reload runnable
	 */
	protected Runnable makeReloadWhiteList(final File list) {
		return new Runnable() {
			private File white_list = list;

			@Override
			public void run() {
				// no white list,warn and quit
				if (this.white_list == null) {
					WhiteListAuthorizer.LOGGER
							.warn("using white list,but not specify the property file,ignore");
					return;
				}

				// load new config
				Properties properties = new Properties();
				try {
					properties.load(new FileInputStream(this.white_list));
				} catch (Exception e) {
					WhiteListAuthorizer.LOGGER.error("fail to load white list:"
							+ this.white_list, e);
				}

				// get the iterator
				Iterator<Entry<Object, Object>> iterator = properties
						.entrySet().iterator();

				// copy the property
				Map<String, Boolean> new_user_list = new ConcurrentHashMap<String, Boolean>();
				Entry<Object, Object> entry = null;
				while (iterator.hasNext()) {
					try {
						entry = iterator.next();
						if (entry != null) {
							new_user_list.put(entry.getKey().toString(),
									Boolean.TRUE);
						}
					} catch (Exception e) {
						WhiteListAuthorizer.LOGGER.warn("unexpeted config:"
								+ entry, e);
					}
				}

				// replace
				WhiteListAuthorizer.this.allow_users = new_user_list;
			}
		};
	}
}
