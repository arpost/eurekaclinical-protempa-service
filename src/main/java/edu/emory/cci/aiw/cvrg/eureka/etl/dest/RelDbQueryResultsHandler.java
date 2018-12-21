package edu.emory.cci.aiw.cvrg.eureka.etl.dest;

import edu.emory.cci.aiw.cvrg.eureka.etl.config.EtlProperties;
import edu.emory.cci.aiw.cvrg.eureka.etl.dao.ETLIdPoolDao;
import edu.emory.cci.aiw.cvrg.eureka.etl.dao.IdPool;
import edu.emory.cci.aiw.cvrg.eureka.etl.entity.RelDbDestinationEntity;
import edu.emory.cci.aiw.cvrg.eureka.etl.entity.RelDbDestinationTableColumnEntity;
import edu.emory.cci.aiw.cvrg.eureka.etl.pool.PoolException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.arp.javautil.arrays.Arrays;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceCache;
import org.protempa.KnowledgeSourceCacheFactory;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinitionCache;
import org.protempa.QueryException;
import org.protempa.dest.AbstractQueryResultsHandler;
import org.protempa.dest.QueryResultsHandlerCloseException;
import org.protempa.dest.QueryResultsHandlerProcessingException;
import org.protempa.dest.table.ConstantColumnSpec;
import org.protempa.dest.table.RelDbTabularWriter;
import org.protempa.dest.table.TabularWriterException;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.value.NominalValue;
import org.protempa.query.Query;
import org.protempa.query.QueryMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Andrew Post
 */
public class RelDbQueryResultsHandler extends AbstractQueryResultsHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RelDbQueryResultsHandler.class);
    
    private final RelDbDestinationEntity config;
    private final Map<String, RelDbTabularWriter> writers;
    private final Map<String, Map<Long, Set<String>>> rowPropositionIdMap;
    private final EtlProperties etlProperties;
    private final KnowledgeSource knowledgeSource;
    private KnowledgeSourceCache ksCache;
    private Map<String, Map<Long, List<FileTableColumnSpecWrapper>>> rowRankToColumnByTableName;
    private final Query query;
    private final Provider<ETLIdPoolDao> idPoolDaoProvider;
    private ETLIdPoolDao idPoolDao;

    public RelDbQueryResultsHandler(Query query,
            RelDbDestinationEntity inRelDbDestinationEntity,
            EtlProperties inEtlProperties, KnowledgeSource inKnowledgeSource,
            Provider<ETLIdPoolDao> inIdPoolDaoProvider) {
        assert inRelDbDestinationEntity != null : "inRelDbDestinationEntity cannot be null";
        this.etlProperties = inEtlProperties;
        this.config = inRelDbDestinationEntity;
        this.knowledgeSource = inKnowledgeSource;
        this.writers = new HashMap<>();
        this.rowPropositionIdMap = new HashMap<>();
        this.rowRankToColumnByTableName = new HashMap<>();
        this.query = query;
        this.idPoolDaoProvider = inIdPoolDaoProvider;
    }

    @Override
    public void start(PropositionDefinitionCache cache) throws QueryResultsHandlerProcessingException {
        this.idPoolDao = this.idPoolDaoProvider.get();
        try {
            this.idPoolDao.start();
        } catch (PoolException ex) {
            throw new QueryResultsHandlerProcessingException("Start query results handler", ex);
        }
        createWriters();
        mapColumnSpecsToColumnNames(cache);
        if (this.query.getQueryMode() == QueryMode.REPLACE) {
            writeHeaders();
        }

        try {
            this.ksCache = new KnowledgeSourceCacheFactory().getInstance(this.knowledgeSource, cache, true);
        } catch (KnowledgeSourceReadException ex) {
            throw new QueryResultsHandlerProcessingException(ex);
        }
    }
    
    @Override
    public void handleQueryResult(String keyId, List<Proposition> propositions, Map<Proposition, Set<Proposition>> forwardDerivations, Map<Proposition, Set<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references) throws QueryResultsHandlerProcessingException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void finish() throws QueryResultsHandlerProcessingException {
    }

    @Override
    public void close() throws QueryResultsHandlerCloseException {
    }
    
    private QueryResultsHandlerCloseException closeWriters(QueryResultsHandlerCloseException exception) {
        if (this.writers != null) {
            for (RelDbTabularWriter writer : this.writers.values()) {
                try {
                    writer.close();
                } catch (TabularWriterException ex) {
                    if (exception != null) {
                        exception.addSuppressed(ex);
                    } else {
                        exception = new QueryResultsHandlerCloseException(ex);
                    }
                }
            }
            this.writers.clear();
        }
        return exception;
    }

    private void writeHeaders() throws AssertionError, QueryResultsHandlerProcessingException {
        for (Map.Entry<String, Map<Long, List<FileTableColumnSpecWrapper>>> me : this.rowRankToColumnByTableName.entrySet()) {
            List<String> columnNames = new ArrayList<>();
            Iterator<List<FileTableColumnSpecWrapper>> iterator = me.getValue().values().iterator();
            if (iterator.hasNext()) {
                for (FileTableColumnSpecWrapper columnSpec : iterator.next()) {
                    try {
                        Arrays.addAll(columnNames, columnSpec.getTableColumnSpec().columnNames(this.knowledgeSource));
                    } catch (KnowledgeSourceReadException ex) {
                        throw new AssertionError("Should never happen");
                    }
                }
            }
            RelDbTabularWriter writer = this.writers.get(me.getKey());
            try {
                for (String columnName : columnNames) {
                    writer.writeNominal(NominalValue.getInstance(columnName));
                }
                writer.newRow();
            } catch (TabularWriterException ex) {
                throw new QueryResultsHandlerProcessingException(ex);
            }
        }
    }

    private void mapColumnSpecsToColumnNames(PropositionDefinitionCache cache) throws QueryResultsHandlerProcessingException {
        this.rowRankToColumnByTableName = new HashMap<>();
        for (RelDbDestinationTableColumnEntity tableColumn : this.config.getTableColumns()) {
            String tableName = tableColumn.getTableName();
            Map<Long, List<FileTableColumnSpecWrapper>> rowRankToTableColumnSpecs = this.rowRankToColumnByTableName.get(tableName);
            if (rowRankToTableColumnSpecs == null) {
                rowRankToTableColumnSpecs = new HashMap<>();
                this.rowRankToColumnByTableName.put(tableName, rowRankToTableColumnSpecs);
            }

            IdPool idPool = idPoolDao != null ? idPoolDao.toIdPool(tableColumn.getIdPool()) : null;
            TableColumnSpecFormat linksFormat
                    = new TableColumnSpecFormat(tableColumn.getColumnName(), tableColumn.getFormat(), idPool);
            try {
                FileTableColumnSpecWrapper tableColumnSpecWrapper = newTableColumnSpec(tableColumn, linksFormat, idPool);
                String pid = tableColumnSpecWrapper.getPropId();
                if (pid != null) {
                    Long rowRank = tableColumn.getRowRank();
                    Map<Long, Set<String>> rowToPropIds = this.rowPropositionIdMap.get(tableName);
                    if (rowToPropIds == null) {
                        rowToPropIds = new HashMap<>();
                        this.rowPropositionIdMap.put(tableName, rowToPropIds);
                    }
                    for (String propId : cache.collectPropIdDescendantsUsingInverseIsA(pid)) {
                        org.arp.javautil.collections.Collections.putSet(rowToPropIds, rowRank, propId);
                    }
                }
                org.arp.javautil.collections.Collections.putList(rowRankToTableColumnSpecs, tableColumn.getRowRank(), tableColumnSpecWrapper);
            } catch (QueryException | ParseException ex) {
                throw new QueryResultsHandlerProcessingException(ex);
            }
        }

        LOGGER.debug("Row concepts: {}", this.rowPropositionIdMap);
    }

    private void createWriters() throws QueryResultsHandlerProcessingException {
        try {
            File outputFileDirectory = this.etlProperties.outputFileDirectory(this.config.getName());
            List<String> tableNames = this.config.getTableColumns()
                    .stream()
                    .map(RelDbDestinationTableColumnEntity::getTableName)
                    .distinct()
                    .collect(Collectors.toCollection(ArrayList::new));
            boolean doAppend = this.query.getQueryMode() != QueryMode.REPLACE;
            if (!doAppend) {
                //truncate or delete from 
                for (File f : outputFileDirectory.listFiles()) {
                    f.delete();
                }
            }
            for (int i = 0, n = tableNames.size(); i < n; i++) {
                String tableName = tableNames.get(i);
                this.writers.put(tableName, new RelDbTabularWriter(
                        new BufferedWriter(new FileWriter(file, doAppend))));
            }
        } catch (IOException ex) {
            throw new QueryResultsHandlerProcessingException(ex);
        }
    }

    private static FileTableColumnSpecWrapper newTableColumnSpec(
            RelDbDestinationTableColumnEntity tableColumn,
            TableColumnSpecFormat linksFormat, IdPool pool) throws ParseException {
        String path = tableColumn.getPath();
        if (path != null) {
            return (FileTableColumnSpecWrapper) linksFormat.parseObject(path);
        } else {
            return new FileTableColumnSpecWrapper(null,
                    new ConstantColumnSpec(tableColumn.getColumnName(), null),
                    new TabularWriterWithPool(pool));
        }
    }
    
}
