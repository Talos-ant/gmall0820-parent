package com.atguigu.gmall.Controller;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.lang.invoke.VarHandle;


/**
 *
 */
@RestController
public class FirstController {

    @RequestMapping("/first")
    public String test(){
        return "this is first";
    }

    
}
