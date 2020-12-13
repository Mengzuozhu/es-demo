package com.mzz.esdemo.common.aspect;

import com.mzz.esdemo.model.Response;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * The type Exception controller advice.
 *
 * @author zuozhu.meng
 */
@ControllerAdvice
public class ExceptionControllerAdvice {

    @ExceptionHandler({Exception.class})
    @ResponseBody
    public Response<?> exceptionHandler(HttpServletRequest httpServletRequest, Exception e) {
        return Response.buildFail(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
    }

}
