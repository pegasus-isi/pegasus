#  Copyright 2009 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import logging

class Logger(logging.getLoggerClass()):
    "A custom logger for Pegasus with TRACE level"
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    TRACE = logging.DEBUG - 1
    NOTSET = logging.NOTSET

    def __init__(self, name, level=0):
        logging.Logger.__init__(self, name, level)

    def trace(self, message, *args, **kwargs):
        "Log a TRACE level message"
        self.log(Logger.TRACE, message, *args, **kwargs)

# Add a TRACE level to logging
logging.addLevelName(Logger.TRACE, "TRACE")

# Use our own logger class, which has trace
logging.setLoggerClass(Logger)

