package org.infinispan.marshall.exts;

import org.infinispan.commons.api.functional.EntryVersion;
import org.infinispan.commons.api.functional.EntryVersion.NumericEntryVersion;
import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.api.functional.MetaParam.MetaCreated;
import org.infinispan.commons.api.functional.MetaParam.MetaEntryVersion;
import org.infinispan.commons.api.functional.MetaParam.MetaLastUsed;
import org.infinispan.commons.api.functional.MetaParam.MetaLifespan;
import org.infinispan.commons.api.functional.MetaParam.MetaMaxIdle;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class MetaParamExternalizers {

   private MetaParamExternalizers() {
      // Do not instantiate
   }

   public static final class LongExternalizer extends AbstractExternalizer<MetaParam<Long>> {
      private final List<Class<? extends MetaParam<Long>>> constructors = Arrays.asList(
            MetaCreated.class, MetaLifespan.class, MetaLastUsed.class, MetaMaxIdle.class);
      private final Map<Class<? extends MetaParam<Long>>, Integer> ids = IntStream.range(0, constructors.size())
            .boxed()
            .collect(Collectors.toMap(constructors::get, UnaryOperator.identity()));

      @Override
      public void writeObject(ObjectOutput output, MetaParam<Long> object) throws IOException {
         output.writeByte(ids.get(object.getClass()));
         output.writeLong(object.get());
      }

      @Override
      public MetaParam<Long> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte type = input.readByte();
         try {
            return constructors.get(type).getConstructor(long.class).newInstance(input.readLong());
         } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new ClassNotFoundException("Type: " + type, e);
         }
      }

      @Override
      public Set<Class<? extends MetaParam<Long>>> getTypeClasses() {
         return ids.keySet();
      }

      @Override
      public Integer getId() {
         return Ids.META_LONG;
      }
   }

   public static final class EntryVersionParamExternalizer extends AbstractExternalizer<MetaEntryVersion> {
      @Override
      public void writeObject(ObjectOutput output, MetaEntryVersion object) throws IOException {
         output.writeObject(object.get());
      }

      @Override
      public MetaEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         EntryVersion entryVersion = (EntryVersion) input.readObject();
         return new MetaEntryVersion(entryVersion);
      }

      @Override
      public Set<Class<? extends MetaEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends MetaEntryVersion>>asSet(MetaEntryVersion.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_ENTRY_VERSION;
      }
   }

   public static final class NumericEntryVersionExternalizer extends AbstractExternalizer<NumericEntryVersion> {
      @Override
      public void writeObject(ObjectOutput output, NumericEntryVersion object) throws IOException {
         output.writeLong(object.get());
      }

      @Override
      public NumericEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         long version = input.readLong();
         return new NumericEntryVersion(version);
      }

      @Override
      public Set<Class<? extends NumericEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends NumericEntryVersion>>asSet(NumericEntryVersion.class);
      }

      @Override
      public Integer getId() {
         return Ids.NUMERIC_ENTRY_VERSION;
      }
   }

}
