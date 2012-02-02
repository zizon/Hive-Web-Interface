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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * md5 digest
 * 
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class MD5 {
	private static final char[] MAP = { '0', '1', '2', '3', '4', '5', '6', '7',
			'8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	private static final MessageDigest DIGEST;
	static {
		try {
			DIGEST = MessageDigest.getInstance("md5");
		} catch (NoSuchAlgorithmException e) {
			// try fail fast
			throw new RuntimeException(e);
		}
	}

	/**
	 * digest the raw to a md5 string to literal representation
	 * 
	 * 
	 * @param raw
	 *            the raw string
	 * @return the md5 literal string
	 */
	public static String digestLiteral(String raw) {
		byte[] md5 = DIGEST.digest(raw.getBytes());

		// the following code is copy from
		// org.apache.commons.codec.digest.DigestUtils
		// with some clean up(remove unnecessary dependency)
		char[] out = new char[md5.length << 1];
		// two characters form the hex value.
		for (int i = 0, j = 0; i < md5.length; i++) {
			out[j++] = MAP[(0xF0 & md5[i]) >>> 4];
			out[j++] = MAP[0x0F & md5[i]];
		}

		return new String(out);
	}

	/**
	 * digest the raw byte
	 * @param raw
	 * 		the raw
	 * @return
	 * 		the digest bytes
	 */
	public static byte[] digest(byte[] raw) {
		return DIGEST.digest(raw);
	}
}
