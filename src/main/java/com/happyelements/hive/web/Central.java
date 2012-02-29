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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * the central thread pool
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class Central {
	private static final Log LOGGER = LogFactory.getLog(Central.class);

	private static final ExecutorService THREAD_POOL = new ThreadPoolExecutor(
			0, Integer.MAX_VALUE, 10L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());

	private static long NOW;

	private static final ScheduledExecutorService TIMER;
	static {
		TIMER = Executors.newScheduledThreadPool(1);

		Central.NOW = System.currentTimeMillis();
		Central.TIMER.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Central.NOW = System.currentTimeMillis();
			}
		}, 0, 1, TimeUnit.SECONDS);
	}

	/**
	 * get the thread pool
	 * @return
	 * 		the thread pool
	 */
	public static ExecutorService getThreadPool() {
		return Central.THREAD_POOL;
	}

	/**
	 * get the timer object
	 * @return
	 * 		the timer
	 */
	public static void schedule(final Runnable runnable, long second_rate) {
		TIMER.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					runnable.run();
				} catch (Throwable e) {
					LOGGER.error("timer exception:", e);
				}
			}
		}, 0, second_rate, TimeUnit.SECONDS);
	}

	/**
	 * get the appropriate now
	 * @return
	 * 		the now time(not much precise)
	 */
	public static long now() {
		return Central.NOW;
	}
}
