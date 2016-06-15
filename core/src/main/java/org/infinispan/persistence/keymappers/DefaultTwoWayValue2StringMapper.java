package org.infinispan.persistence.keymappers;

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.InitializationContextAware;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An extension of {@link DefaultTwoWayKey2StringMapper} that is also suitable
 * for values, not only keys; the {@link #getStringMapping(Object)} method will
 * only return {@link String} representation, if it can, while the
 * {@link #getObjectsMapping(Object)} will return a single-element array with
 * the value's marshaled form.
 *
 * @author Krzysztof Sobolewski
 *
 * @since 9.0.0
 */
public class DefaultTwoWayValue2StringMapper extends DefaultTwoWayKey2StringMapper implements InitializationContextAware {
   private static final Log log = LogFactory.getLog(DefaultTwoWayValue2StringMapper.class);
   private InitializationContext ctx;

   @Override
   public void setInitializationContext(InitializationContext ctx) {
      this.ctx = ctx;
   }

   @Override
   public Object[] getObjectsMapping(Object key) {
      KeyValuePair<?, ?> obj = new KeyValuePair<>(((MarshalledEntry<?, ?>) key).getValueBytes(), ((MarshalledEntry<?, ?>) key).getMetadataBytes());
      try {
         return new Object[] { ctx.getMarshaller().objectToByteBuffer(obj) };
      } catch (IOException | InterruptedException e) {
         throw new IllegalArgumentException(e);
      }
   }

   @Override
   public Object getKeyMapping(Object[] objectKey) {
      try {
         KeyValuePair<ByteBuffer, ByteBuffer> icv;
         if (objectKey[1] instanceof Blob) {
            icv = (KeyValuePair<ByteBuffer, ByteBuffer>)
                  ctx.getMarshaller().objectFromInputStream(((Blob) objectKey[1]).getBinaryStream());
         } else if (objectKey[1] instanceof byte[]) {
            icv = (KeyValuePair<ByteBuffer, ByteBuffer>)
                  ctx.getMarshaller().objectFromByteBuffer((byte[]) objectKey[1]);
         } else {
            return super.getKeyMapping(objectKey);
         }
         return ctx.getMarshalledEntryFactory().newMarshalledEntry(objectKey[0], icv.getKey(), icv.getValue());
      } catch (IOException | SQLException e) {
         throw new PersistenceException("Error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         throw new PersistenceException("*UNEXPECTED* ClassNotFoundException. This should not happen as Bucket class exists", e);
      }
   }

   @Override
   public boolean isSupportedType(Class<?> keyType) {
      return true;
   }
}
