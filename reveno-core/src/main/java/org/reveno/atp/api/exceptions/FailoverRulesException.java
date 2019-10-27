package org.reveno.atp.api.exceptions;

/**
 * 故障转移规则异常
 */
public class FailoverRulesException extends RuntimeException {

    public FailoverRulesException(String message) {
        super(message);
    }

}
