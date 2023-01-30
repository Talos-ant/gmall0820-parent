package com.atguigu.gmall.controller;



import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


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
