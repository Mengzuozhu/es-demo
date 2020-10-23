package com.mzz.esdemo.model;

import lombok.Data;

/**
 * The type Response.
 *
 * @param <T> the type parameter
 * @author zuozhu.meng
 * @since 2020 /10/22
 */
@Data
public class Response<T> {
    private boolean isSuccess;

    private Integer errCode;

    private String errMessage;

    private T data;

    /**
     * Of response.
     *
     * @param <T>  the type parameter
     * @param data the data
     * @return the response
     */
    public static <T> Response<T> of(T data) {
        return buildSuccess(data);
    }

    /**
     * Build success response.
     *
     * @param <T>  the type parameter
     * @param data the data
     * @return the response
     */
    public static <T> Response<T> buildSuccess(T data) {
        Response<T> response = new Response<>();
        response.setSuccess(true);
        response.setData(data);
        return response;
    }

    /**
     * Build fail response.
     *
     * @param errCode    the err code
     * @param errMessage the err message
     * @return the response
     */
    public static Response<?> buildFail(Integer errCode, String errMessage) {
        Response<?> response = new Response<>();
        response.setSuccess(false);
        response.setErrCode(errCode);
        response.setErrMessage(errMessage);
        return response;
    }

}
