package org.infinispan.all.embeddedquery;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;
import org.hibernate.search.engine.integration.impl.ExtendedSearchIntegrator;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.transaction.TransactionMode;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Abstract class for query tests for uber jars.
 *
 * @author Jiri Holusa (jholusa@redhat.com)
 */
public abstract class AbstractQueryTest {

   protected static Cache<Object, Object> cache;

   protected static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer();

   protected static EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcfg = new GlobalConfigurationBuilder();
      gcfg.globalJmxStatistics().allowDuplicateDomains(true);

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.transaction()
          .transactionMode(TransactionMode.TRANSACTIONAL)
          .indexing()
          .index(Index.ALL)
          .addProperty("default.directory_provider", "ram")
          .addProperty("error_handler", "org.infinispan.all.embeddedquery.testdomain.StaticTestingErrorHandler")
          .addProperty("lucene_version", "LUCENE_CURRENT");

      EmbeddedCacheManager cm = new DefaultCacheManager(gcfg.build(), cfg.build());

      return cm;
   }

   protected CacheQuery createCacheQuery(Cache m_cache, String fieldName, String searchString) {
      QueryBuilder queryBuilder = new QueryBuilder(STANDARD_ANALYZER);
      Query query = queryBuilder.createBooleanQuery(fieldName, searchString);
      SearchManager queryFactory = Search.getSearchManager(m_cache);
      CacheQuery cacheQuery = queryFactory.getQuery(query);

      return cacheQuery;
   }

   protected void assertIndexingKnows(Cache<Object, Object> cache, Class... types) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);
      assertNotNull(searchIntegrator);

      Map<Class<?>, EntityIndexBinding> indexBindingForEntity = searchIntegrator.unwrap(ExtendedSearchIntegrator.class).getIndexBindings();
      assertNotNull(indexBindingForEntity);

      Set<Class<?>> keySet = indexBindingForEntity.keySet();
      assertEquals(types.length, keySet.size());
      assertTrue(keySet.containsAll(Arrays.asList(types)));
   }

   protected static BasicCache<Object, Object> getCacheForWrite() {
      return getCacheForQuery();
   }

   protected static BasicCache<Object, Object> getCacheForQuery() {
      return cache;
   }

   protected static QueryFactory getQueryFactory() {
      return Search.getQueryFactory((Cache) getCacheForQuery());
   }

}
