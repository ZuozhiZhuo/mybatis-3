/*
 *    Copyright 2009-2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.mapping;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.builder.InitializingObject;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;
import org.apache.ibatis.cache.decorators.BlockingCache;
import org.apache.ibatis.cache.decorators.LoggingCache;
import org.apache.ibatis.cache.decorators.LruCache;
import org.apache.ibatis.cache.decorators.ScheduledCache;
import org.apache.ibatis.cache.decorators.SerializedCache;
import org.apache.ibatis.cache.decorators.SynchronizedCache;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;

/**
 * @author Clinton Begin
 */
public class CacheBuilder {
  private final String id;
  private Class<? extends Cache> implementation;
  private final List<Class<? extends Cache>> decorators;
  private Integer size;
  private Long clearInterval;
  private boolean readWrite;

  /**
   * <cache> 节点下所有的 <property> 节点数据
   */
  private Properties properties;
  private boolean blocking;

  public CacheBuilder(String id) {
    this.id = id;
    this.decorators = new ArrayList<>();
  }

  public CacheBuilder implementation(Class<? extends Cache> implementation) {
    this.implementation = implementation;
    return this;
  }

  public CacheBuilder addDecorator(Class<? extends Cache> decorator) {
    if (decorator != null) {
      this.decorators.add(decorator);
    }
    return this;
  }

  public CacheBuilder size(Integer size) {
    this.size = size;
    return this;
  }

  public CacheBuilder clearInterval(Long clearInterval) {
    this.clearInterval = clearInterval;
    return this;
  }

  public CacheBuilder readWrite(boolean readWrite) {
    this.readWrite = readWrite;
    return this;
  }

  public CacheBuilder blocking(boolean blocking) {
    this.blocking = blocking;
    return this;
  }

  public CacheBuilder properties(Properties properties) {
    this.properties = properties;
    return this;
  }

  public Cache build() {
    //设置默认实现、添加默认装饰器类
    setDefaultImplementations();
    //根据指定的实现初始化缓存实例
    Cache cache = newBaseCacheInstance(implementation, id);
    //将 <cache> 节点下设置的 <property> 属性值赋值到cache的Field中
    setCacheProperties(cache);
    // issue #352, do not apply decorators to custom caches
    //仅对内置缓存 PerpetualCache 应用装饰器
    if (PerpetualCache.class.equals(cache.getClass())) {
      //遍历装饰器，使用装饰器类层层包裹缓存实例，增强缓存功能
      for (Class<? extends Cache> decorator : decorators) {
        //利用反射实现装饰器实例
        /*
        为什么用反射，而不是直接new一个对象？
        装饰器模式下，必须要装饰一个Cache对象，Cache 的域必须是final的，构造对象时必须要指定cache对象，因为这些装饰类都是后期
        添加的，因此框架中无法编写具体类的实例化代码，，所以只能用反射来进行初始化
         */
        cache = newCacheDecoratorInstance(decorator, cache);
        //将 <cache> 节点下设置的 <property> 属性值赋值到cache的Field中
        setCacheProperties(cache);
      }
      //为缓存设置系统中标准的装饰器
      cache = setStandardDecorators(cache);
    } else if (!LoggingCache.class.isAssignableFrom(cache.getClass())) { // 如果cache不是LoggingCache的子类
      //应用具有日志功能的缓存装饰器
      cache = new LoggingCache(cache);
    }
    return cache;
  }

  private void setDefaultImplementations() {
    if (implementation == null) {
      implementation = PerpetualCache.class;
      if (decorators.isEmpty()) {
        decorators.add(LruCache.class);
      }
    }
  }

  private Cache setStandardDecorators(Cache cache) {
    try {
      //获取缓存对象元信息
      //在这里MetaObject的作用判断是否有 size 的setter，并反射赋值
      MetaObject metaCache = SystemMetaObject.forObject(cache);
      //如果设置了size属性并且缓存有 size 的 setter，则进行反射赋值
      if (size != null && metaCache.hasSetter("size")) {
        //反射赋值
        metaCache.setValue("size", size);
      }
      if (clearInterval != null) {
        // 创建定时清除缓存装饰器
        cache = new ScheduledCache(cache);
        //设置定时清除缓存间隔
        ((ScheduledCache) cache).setClearInterval(clearInterval);
      }
      if (readWrite) {
        //设置序列化对象装饰器，实现对象只读，详见 SerializedCache
        cache = new SerializedCache(cache);
      }
      // 设置日志缓存装饰器
      cache = new LoggingCache(cache);
      // 设置同步缓存装饰器，保证存储、获取对象同步
      cache = new SynchronizedCache(cache);
      if (blocking) {
        // 设置线程阻塞缓存装饰器，保证只有一个线程到数据库中查找指定key对应的数据
        cache = new BlockingCache(cache);
      }
      return cache;
    } catch (Exception e) {
      throw new CacheException("Error building standard cache decorators.  Cause: " + e, e);
    }
  }

  // 使用第三方缓存时，有很多属性需要配置，他们配置 <cache> 节点的 <property> ，这个方法就是把这些property赋值到自定义cache中
  private void setCacheProperties(Cache cache) {
    if (properties != null) {
      // 获取该缓存的元对象，可能不是那么好理解，一般 “元” 的概念就是获取对象本身信息
      // 在这里获取元对象的目的是为了判断该Cache对象有没有某个property属性的setter方法，如果有就反射调用setter方法；判断是否
      // 拥有setter方法和反射调用setter方法就是 MetaObject 为我们提供的功能
      //TODO 这个函数需要深入研究
      MetaObject metaCache = SystemMetaObject.forObject(cache);
      //遍历所有Property
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        String name = (String) entry.getKey();
        String value = (String) entry.getValue();
        //通过元对象判断该property在此cache中能否找到getter对象，如果找的到就将数据赋值到该缓存该Field中
        if (metaCache.hasSetter(name)) {
          //获取setterType
          Class<?> type = metaCache.getSetterType(name);
          //逐一判断setter参数是哪种数据类型，如果匹配则进行强制转换并使用反射赋值
          if (String.class == type) {
            metaCache.setValue(name, value);
          } else if (int.class == type
              || Integer.class == type) {
            metaCache.setValue(name, Integer.valueOf(value));
          } else if (long.class == type
              || Long.class == type) {
            metaCache.setValue(name, Long.valueOf(value));
          } else if (short.class == type
              || Short.class == type) {
            metaCache.setValue(name, Short.valueOf(value));
          } else if (byte.class == type
              || Byte.class == type) {
            metaCache.setValue(name, Byte.valueOf(value));
          } else if (float.class == type
              || Float.class == type) {
            metaCache.setValue(name, Float.valueOf(value));
          } else if (boolean.class == type
              || Boolean.class == type) {
            metaCache.setValue(name, Boolean.valueOf(value));
          } else if (double.class == type
              || Double.class == type) {
            metaCache.setValue(name, Double.valueOf(value));
          } else {
            //如果数据不是其中任何一种，就直接抛出异常，表示不支持的数据类型
            throw new CacheException("Unsupported property type for cache: '" + name + "' of type " + type);
          }
        }
      }
    }
    // 如果缓存实现了 InitializingObject 接口
    // 则调用initialize方法进行初始化
    if (InitializingObject.class.isAssignableFrom(cache.getClass())) {
      try {
        ((InitializingObject) cache).initialize();
      } catch (Exception e) {
        throw new CacheException("Failed cache initialization for '"
          + cache.getId() + "' on '" + cache.getClass().getName() + "'", e);
      }
    }
  }

  private Cache newBaseCacheInstance(Class<? extends Cache> cacheClass, String id) {
    //获取到带一个参数的构造器
    Constructor<? extends Cache> cacheConstructor = getBaseCacheConstructor(cacheClass);
    try {
      //实例化构造器
      return cacheConstructor.newInstance(id);
    } catch (Exception e) {
      //实例失败抛出异常
      throw new CacheException("Could not instantiate cache implementation (" + cacheClass + "). Cause: " + e, e);
    }
  }

  private Constructor<? extends Cache> getBaseCacheConstructor(Class<? extends Cache> cacheClass) {
    try {
      //获取只有一个String参数的构造器，如果获取不到则抛出异常，告知用户需要实现这样的一个构造器
      return cacheClass.getConstructor(String.class);
    } catch (Exception e) {
      //抛出异常
      throw new CacheException("Invalid base cache implementation (" + cacheClass + ").  "
        + "Base cache implementations must have a constructor that takes a String id as a parameter.  Cause: " + e, e);
    }
  }

  private Cache newCacheDecoratorInstance(Class<? extends Cache> cacheClass, Cache base) {
    Constructor<? extends Cache> cacheConstructor = getCacheDecoratorConstructor(cacheClass);
    try {
      return cacheConstructor.newInstance(base);
    } catch (Exception e) {
      throw new CacheException("Could not instantiate cache decorator (" + cacheClass + "). Cause: " + e, e);
    }
  }

  private Constructor<? extends Cache> getCacheDecoratorConstructor(Class<? extends Cache> cacheClass) {
    try {
      return cacheClass.getConstructor(Cache.class);
    } catch (Exception e) {
      throw new CacheException("Invalid cache decorator (" + cacheClass + ").  "
        + "Cache decorators must have a constructor that takes a Cache instance as a parameter.  Cause: " + e, e);
    }
  }
}
