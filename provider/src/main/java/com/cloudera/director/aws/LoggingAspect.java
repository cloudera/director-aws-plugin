// (c) Copyright 2017 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.director.aws;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aspect to log rpc calls.
 */
@Aspect
public class LoggingAspect {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingAspect.class);
  private static final int LOG_LENGTH_LIMIT = 200;

  /**
   * EC2 calls.
   */
  @Pointcut("call(public * com.amazonaws.services.ec2.AmazonEC2Client.*(..))")
  public void ec2Call() {
  }

  /**
   * IAM calls.
   */
  @Pointcut("call(public * com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient.*(..))")
  public void iamCall() {
  }

  /**
   * KMS calls.
   */
  @Pointcut("call(public * com.amazonaws.services.kms.AWSKMSClient.*(..))")
  public void kmsCall() {
  }

  /**
   * RDS calls.
   */
  @Pointcut("call(public * com.amazonaws.services.rds.AmazonRDSClient.*(..))")
  public void rdsCall() {
  }

  /**
   * Logging advice around join point.
   *
   * @param jp join point.
   * @return original method returns.
   * @throws Throwable original method throwables.
   */
  @Around("ec2Call() || iamCall() || kmsCall() || rdsCall()")
  public Object log(ProceedingJoinPoint jp) throws Throwable {
    MethodSignature ms = MethodSignature.class.cast(jp.getSignature());
    String methodName = ms.getDeclaringType().getSimpleName() + "::" + ms.getMethod().getName();

    LOG.debug("-> Calling {} with argument(s): {}", methodName, jp.getArgs());

    try {
      Object result = jp.proceed();
      LOG.debug("<- {} returns: {}", methodName, truncate(result));
      return result;

    } catch (Throwable e) {
      LOG.debug("<- {} throws {}", methodName, e.getMessage());
      throw e;
    }
  }

  private String truncate(Object result) {
    if (result == null) {
      return null;
    }

    String s = result.toString();
    String prefix = "";
    int length = s.length();

    if (length > LOG_LENGTH_LIMIT) {
      prefix = "...";
      length = LOG_LENGTH_LIMIT;
    }

    return s.substring(0, length) + prefix;
  }
}
