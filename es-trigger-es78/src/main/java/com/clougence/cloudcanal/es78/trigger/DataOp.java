package com.clougence.cloudcanal.es78.trigger;

import lombok.Getter;

/**
 * @author bucketli 2024/7/30 16:04:54
 */
public enum DataOp {

    INSERT(1),
    UPDATE(2),
    DELETE(3);

    @Getter
    private final int opInt;

    DataOp(int opInt){
        this.opInt = opInt;
    }
}
