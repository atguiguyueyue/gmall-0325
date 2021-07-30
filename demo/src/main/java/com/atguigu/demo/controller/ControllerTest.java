package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
//@RestController = @Controller+@ResponseBody
@RestController
public class ControllerTest {

    //用来处理某个请求的方法
    @RequestMapping("test1")
    public String test(){
        System.out.println("123");
        return "success";
    }

    @RequestMapping("test2")
    public String test1(@RequestParam("a") Integer age, @RequestParam("na") String name){
        System.out.println("aaa");
        return "name:" + name + "age:" + age;
    }

}
