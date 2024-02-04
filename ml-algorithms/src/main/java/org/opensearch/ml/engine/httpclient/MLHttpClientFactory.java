/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.engine.httpclient;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import org.apache.commons.lang3.math.NumberUtils;

import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

@Log4j2
public class MLHttpClientFactory {

    public static SdkAsyncHttpClient getAsyncHttpClient() {
        try {
            return AccessController
                .doPrivileged(
                    (PrivilegedExceptionAction<SdkAsyncHttpClient>) () -> NettyNioAsyncHttpClient.builder().maxConcurrency(100).build()
                );
        } catch (PrivilegedActionException e) {
            return null;
        }
    }

    public static void validate(String protocol, String host, String port) throws UnknownHostException {
        if (protocol != null && !"http".equalsIgnoreCase(protocol) && !"https".equals(protocol)) {
            log.error("Remote inference protocol is not http or https: " + protocol);
            throw new IllegalArgumentException("Protocol is not http or https: " + protocol);
        }
        String portStr = Optional.ofNullable(port).orElseGet(() -> {
            if (protocol == null || "http".equals(protocol.toLowerCase(Locale.getDefault()))) {
                return "80";
            } else {
                return "443";
            }
        });
        if (!NumberUtils.isDigits(portStr)) {
            log.error("Remote inference port is not a valid number: " + portStr);
            throw new IllegalArgumentException("Port is not a valid number: " + portStr);
        }
        int portNum = Integer.parseInt(portStr);
        if (portNum < 0 || portNum > 65536) {
            log.error("Remote inference port out of range: " + port);
            throw new IllegalArgumentException("Port out of range: " + port);
        }
        validateIp(host);
    }

    private static void validateIp(String hostName) throws UnknownHostException {
        InetAddress[] addresses = InetAddress.getAllByName(hostName);
        if (hasPrivateIpAddress(addresses)) {
            log.error("Remote inference host name has private ip address: " + hostName);
            throw new IllegalArgumentException("Remote inference host name has private ip address: " + hostName);
        }
    }

    private static boolean hasPrivateIpAddress(InetAddress[] ipAddress) {
        for (InetAddress ip : ipAddress) {
            if (ip instanceof Inet4Address) {
                byte[] bytes = ip.getAddress();
                if (bytes.length != 4) {
                    return true;
                } else {
                    int firstOctets = bytes[0] & 0xff;
                    int firstInOctal = parseWithOctal(String.valueOf(firstOctets));
                    int firstInHex = Integer.parseInt(String.valueOf(firstOctets), 16);
                    if (firstInOctal == 127 || firstInHex == 127) {
                        return bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 1;
                    } else if (firstInOctal == 10 || firstInHex == 10) {
                        return true;
                    } else if (firstInOctal == 172 || firstInHex == 172) {
                        int secondOctets = bytes[1] & 0xff;
                        int secondInOctal = parseWithOctal(String.valueOf(secondOctets));
                        int secondInHex = Integer.parseInt(String.valueOf(secondOctets), 16);
                        return (secondInOctal >= 16 && secondInOctal <= 32) || (secondInHex >= 16 && secondInHex <= 32);
                    } else if (firstInOctal == 192 || firstInHex == 192) {
                        int secondOctets = bytes[1] & 0xff;
                        int secondInOctal = parseWithOctal(String.valueOf(secondOctets));
                        int secondInHex = Integer.parseInt(String.valueOf(secondOctets), 16);
                        return secondInOctal == 168 || secondInHex == 168;
                    }
                }
            }
        }
        return Arrays.stream(ipAddress).anyMatch(x -> x.isSiteLocalAddress() || x.isLoopbackAddress() || x.isAnyLocalAddress());
    }

    private static int parseWithOctal(String input) {
        try {
            return Integer.parseInt(input, 8);
        } catch (NumberFormatException e) {
            return Integer.parseInt(input);
        }
    }
}
