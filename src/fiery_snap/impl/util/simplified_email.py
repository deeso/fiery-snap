# Adam Pridgen
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import smtplib
import sys
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = None

class Logger(object):

    @classmethod
    def create_logger(cls, name=None, level=logging.DEBUG,
                      fmt='[%(asctime)s - %(name)s] %(message)s'):

        logger = logging.getLogger()
        if name is not None:
            logger = logging.getLogger(name)

        logger.setLevel(level)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger


class SendEmail(object):

    def setupLogging(self, logger_name, level):
        return Logger.create_logger(name='SendEmail', level=level)

    def __init__(self, host, port=25, user=None, password=None,
                 use_tls=True, level=logging.DEBUG, logger_name="SendEmail"):

        self.host = host
        self.port = port
        self.use_tls = use_tls
        self.user = user
        self.password = password
        self.level = level
        self.logger = self.setupLogging(logger_name, level)

    def send_email(self, sender, recipients, message):
        self.logger.log(self.level, "Sending message to server: %s:%s"%(self.host, self.port))
        server = smtplib.SMTP()
        #server.set_debuglevel(self.level)
        server.connect(self.host, self.port)
        if self.use_tls:
            self.logger.log(self.level, "Starting TLS")
            server.starttls()

        if self.user is not None and self.password is not None:
            self.logger.log(self.level, "Authenticating as %s"%self.user)
            server.login(self.user, self.password)

        #elif self.password is not None:
        #    self.logger.log(self.level, "Authenticating as %s"%self.sender)
        #    server.login(sender, self.password)

        self.logger.log(self.level, "Sending email as %s to %d recipients"% (sender, len(recipients)))
        server.sendmail(sender, recipients, message)
        self.logger.log(self.level, "Send complete")
        server.quit()
        return True

    def send(self, sender, recipient, message, cc=[], bcc=[]):
        recipients = []
        if recipient != sender:
            recipient.append(recipient)
        recipients = recipients + cc + bcc
        if message is None:
            x = "Message must be a string or a MIMEmultipart instance"
            self.logger.log(self.level, "Failed to send email: %s"% (x))
            raise Exception(x)
        msg_str = message

        if not isinstance(message, str):
            msg_str = message.as_string()

        return self.send_email(sender, recipients, msg_str)

    def send_mime_message(self, sender, recipient=None, cc=[], bcc=[],
                           subject='', encoding='utf-8',
                           body_content_type='plain', body='',
                           attachment_name=None, attachment=None,
                           attachment_content_type=None):
        recipient = sender if recipient is None else recipient
        self.logger.log(self.level, "Building message from: %s --> %s"% (sender, recipient))
        message = MIMEMultipart()
        if attachment is None:
            message = MIMEText(body.encode(encoding), body_content_type, encoding)

        message['From'] = sender
        message['To'] = recipient
        message['Subject'] = subject
        message['Cc'] = ",".join(cc)
        message['Bcc'] = bcc
        if attachment is not None:
            message.attach(content)
        aname = attachment_name

        if aname is None:
            aname = "unknown"

        atype = attachment_content_type
        if atype is None:
            atype = "unknown"

        if attachment is not None:
            self.logger.log(self.level, "Adding attachment to email len=%d"% (len(attachment)))
            a = MIMEApplication(attachment, Name=aname)
            a['Content-Disposition'] = 'attachment; filename="%s"' % atype
            message.attach(a)

        return self.send(sender, recipient, message, cc=cc, bcc=bcc)
