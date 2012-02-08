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

/**
 * base64 encode and decode
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class Base64 {

	private static final char[] MAPPING = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
			.toCharArray();

	/**
	 * encode bytes to base64 chars
	 * @param raw
	 * 		the raw bytes
	 * @return
	 * 		the base64 encoded string repression
	 */
	public static String encode(byte[] raw) {
		if (raw == null) {
			return "";
		}

		StringBuilder buffer = new StringBuilder((1 + (raw.length / 3)) * 3);
		int tracker = 0;
		while (tracker + 3 <= raw.length) {
			buffer.append(MAPPING[(raw[tracker] & 0xff) >> 2])
					.append(MAPPING[((raw[tracker] & 0x03) << 4)
							| (raw[tracker + 1] & 0xf0) >> 4]) //
					.append(MAPPING[((raw[tracker + 1] & 0x0f) << 2)
							| ((raw[tracker + 2]) & 0xc0) >> 6])//
					.append(MAPPING[raw[tracker + 2] & 0x3f]);
			tracker += 3;
		}

		if (tracker < raw.length) {
			switch (raw.length - tracker) {
			case 1:
				buffer.append(MAPPING[raw[tracker] >> 2]) //
						.append(MAPPING[((raw[tracker] & 0x3) << 4)]) //
						.append('=').append('=');
				break;
			case 2:
				buffer.append(MAPPING[raw[tracker] >> 2])
						.append(MAPPING[((raw[tracker] & 0x3) << 4)
								| (raw[tracker + 1] & 0xf0) >> 4]) //
						.append(MAPPING[((raw[tracker + 1] & 0xf) << 2)]) //
						.append('=');
				break;
			default:
				throw new RuntimeException(
						"unexpected base64 enconde,should not here");
			}
		}

		return buffer.toString();
	}

	/**
	 * decode a base64 string to bytes
	 * @param raw
	 * 		the base64 encoded string
	 * @return
	 * 		the bytes
	 * @throws IllegalArgumentException
	 * 		throw when encounter illegal character
	 */
	public static byte[] decode(String raw) throws IllegalArgumentException {
		int length = 0;
		if (raw == null || (length = raw.length()) == 0 || length % 4 != 0) {
			throw new IllegalArgumentException(
					"input should not be null and should be mod 4");
		}

		byte[] buffer = null;
		if (raw.charAt(length - 1) == '=') {
			if (raw.charAt(length - 2) == '=') {
				buffer = new byte[length / 4 * 3 - 2];
			} else {
				buffer = new byte[length / 4 * 3 - 1];
			}
		} else {
			buffer = new byte[length / 4 * 3];
		}

		int tracker = 0;
		byte b1 = 0;
		byte b2 = 0;
		byte b3 = 0;
		byte b4 = 0;
		int i = 0;
		while (i + 3 <= buffer.length) {
			b1 = translate(raw.charAt(tracker));
			b2 = translate(raw.charAt(tracker + 1));
			b3 = translate(raw.charAt(tracker + 2));
			b4 = translate(raw.charAt(tracker + 3));

			buffer[i++] = (byte) ((b1 << 2) | ((b2 >> 4) & 0x3));
			buffer[i++] = (byte) (((b2 & 0xf) << 4) | ((b3 >> 2) & 0xf));
			buffer[i++] = (byte) (((b3 & 0x3) << 6) | b4);
			tracker += 4;
		}

		switch (buffer.length - i) {
		case 0:
			break;
		case 1:
			b1 = translate(raw.charAt(tracker));
			b2 = translate(raw.charAt(tracker + 1));
			buffer[i++] = (byte) ((b1 << 2) | ((b2 >> 4) & 0x3));
			break;
		case 2:
			b1 = translate(raw.charAt(tracker));
			b2 = translate(raw.charAt(tracker + 1));
			buffer[i++] = (byte) ((b1 << 2) | ((b2 >> 4) & 0x3));
			b3 = translate(raw.charAt(tracker + 2));
			buffer[i] = (byte) (((b2 & 0xf) << 4) | ((b3 >> 2) & 0xf));
			break;
		default:
			throw new IllegalArgumentException(
					"unexpected base64 enconde,should not here");
		}
		return buffer;
	}

	/**
	 * translate base64 character to byte
	 * @param character
	 * 		the base64 char
	 * @return
	 * 		the byte representation
	 */
	private static byte translate(char character) {
		switch (character) {
		case '+':
			return 62;
		case '/':
			return 63;
		default:
			if ('a' <= character && character <= 'z') {
				return (byte) (character - 'a' + 26);
			} else if ('A' <= character && character <= 'Z') {
				return (byte) (character - 'A');
			} else if ('0' <= character && character <= '9') {
				return (byte) (character - '0' + 52);
			}
		case '=':
			throw new IllegalArgumentException(
					"could not translate character '" + character + "'");
		}
	}
	
	public static void main(String[] args) {
		System.out.println("	 ".charAt(0) == '\t');
		System.out.println( new String( decode("ZmVuZ2xlaS55b3U6eW91ZmVuZ2xlaTEy0zQ=")));
	}
}
