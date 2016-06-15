package org.infinispan.persistence.spi;

import org.infinispan.persistence.keymappers.Key2StringMapper;

/**
 * If a {@link Key2StringMapper} implements this interface, an
 * {@link InitializationContext} will be injected into it.
 * 
 * @author Krzysztof Sobolewski
 * @since 9.0.0
 */
public interface InitializationContextAware {
   void setInitializationContext(InitializationContext ctx);
}
