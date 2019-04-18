/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.scheduler;

import java.util.Objects;

public class MailConfig {

    private String smtpHost;
    private int port;
    private String userEmail;
    private String password;
    private String fromName;
    private String fromEmail;
    private String replyTo;

    public String getSmtpHost() {
        return smtpHost;
    }

    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getFromName() {
        return fromName;
    }

    public void setFromName(String fromName) {
        this.fromName = fromName;
    }

    public String getFromEmail() {
        return fromEmail;
    }

    public void setFromEmail(String fromEmail) {
        this.fromEmail = fromEmail;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MailConfig that = (MailConfig) o;
        return port == that.port &&
                Objects.equals(smtpHost, that.smtpHost) &&
                Objects.equals(userEmail, that.userEmail) &&
                Objects.equals(password, that.password) &&
                Objects.equals(fromName, that.fromName) &&
                Objects.equals(fromEmail, that.fromEmail) &&
                Objects.equals(replyTo, that.replyTo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(smtpHost, port, userEmail, password, fromName, fromEmail, replyTo);
    }

    @Override
    public String toString() {
        return "MailConfig{" +
                "smtpHost='" + smtpHost + '\'' +
                ", port=" + port +
                ", userEmail='" + userEmail + '\'' +
                ", password='" + password + '\'' +
                ", fromName='" + fromName + '\'' +
                ", fromEmail='" + fromEmail + '\'' +
                ", replyTo='" + replyTo + '\'' +
                '}';
    }
}
