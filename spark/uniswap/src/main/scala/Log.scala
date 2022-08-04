package fyi.dap.uniswap

import org.apache.log4j.Logger

object Log {

    val logger = Logger.getRootLogger
    def debug(s: String) = logger.debug(s)
    def info(s: String) = logger.info(s)
    def warn(s: String) = logger.warn(s)
    def error(s: String) = logger.error(s)

}

