package edu.emory.cci.aiw.cvrg.eureka.etl.dest;

/*
 * #%L
 * Eureka Protempa ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
 * %%
 * This program is dual licensed under the Apache 2 and GPLv3 licenses.
 * 
 * Apache License, Version 2.0:
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * GNU General Public License version 3:
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */
import edu.emory.cci.aiw.cvrg.eureka.etl.entity.TabularFileDestinationEntity;
import edu.emory.cci.aiw.cvrg.eureka.etl.entity.TabularFileDestinationTableColumnEntity;
import edu.emory.cci.aiw.cvrg.eureka.etl.config.EtlProperties;
import edu.emory.cci.aiw.cvrg.eureka.etl.dao.ETLIdPoolDao;
import edu.emory.cci.aiw.cvrg.eureka.etl.dao.IdPool;
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
import org.arp.javautil.collections.Collections;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceCache;
import org.protempa.KnowledgeSourceCacheFactory;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinitionCache;
import org.protempa.QueryException;
import org.protempa.dest.AbstractQueryResultsHandler;
import org.protempa.dest.QueryResultsHandlerCloseException;
import org.protempa.dest.QueryResultsHandlerProcessingException;
import org.protempa.dest.QueryResultsHandlerValidationFailedException;
import org.protempa.dest.table.ConstantColumnSpec;
import org.protempa.dest.table.FileTabularWriter;
import org.protempa.dest.table.QuoteModel;
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
public class TabularFileQueryResultsHandler extends AbstractQueryResultsHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TabularFileQueryResultsHandler.class);

    private final TabularFileDestinationEntity config;
    private final Map<String, FileTabularWriter> writers;
    private final Map<String, Map<Long, Set<String>>> rowPropositionIdMap;
    private final EtlProperties etlProperties;
    private final KnowledgeSource knowledgeSource;
    private final char delimiter;
    private KnowledgeSourceCache ksCache;
    private Map<String, Map<Long, List<FileTableColumnSpecWrapper>>> rowRankToColumnByTableName;
    private final Query query;
    private final Provider<ETLIdPoolDao> idPoolDaoProvider;
    private ETLIdPoolDao idPoolDao;

    TabularFileQueryResultsHandler(Query query,
            TabularFileDestinationEntity inTabularFileDestinationEntity,
            EtlProperties inEtlProperties, KnowledgeSource inKnowledgeSource,
            Provider<ETLIdPoolDao> inIdPoolDaoProvider) {
        assert inTabularFileDestinationEntity != null : "inTabularFileDestinationEntity cannot be null";
        this.etlProperties = inEtlProperties;
        this.config = inTabularFileDestinationEntity;
        this.knowledgeSource = inKnowledgeSource;
        Character delim = inTabularFileDestinationEntity.getDelimiter();
        if (delim != null) {
            this.delimiter = delim;
        } else {
            this.delimiter = '\t';
        }
        this.writers = new HashMap<>();
        this.rowPropositionIdMap = new HashMap<>();
        this.rowRankToColumnByTableName = new HashMap<>();
        this.query = query;
        this.idPoolDaoProvider = inIdPoolDaoProvider;
    }

    @Override
    public void validate() throws QueryResultsHandlerValidationFailedException {
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
    public void handleQueryResult(String keyId, List<Proposition> propositions,
            Map<Proposition, Set<Proposition>> forwardDerivations,
            Map<Proposition, Set<Proposition>> backwardDerivations,
            Map<UniqueId, Proposition> references) throws QueryResultsHandlerProcessingException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Data for keyId {}: {}", new Object[]{keyId, propositions});
        }

        Map<String, List<Proposition>> collect = new HashMap<>();
        for (Proposition prop : propositions) {
            Collections.putList(collect, prop.getId(), prop);
        }

        for (Map.Entry<String, Map<Long, List<FileTableColumnSpecWrapper>>> tableNameToRowNumToColumnSpecs : this.rowRankToColumnByTableName.entrySet()) {
            String tableName = tableNameToRowNumToColumnSpecs.getKey();
            Map<Long, List<FileTableColumnSpecWrapper>> rowNumToColumnSpecs = tableNameToRowNumToColumnSpecs.getValue();
            for (Map.Entry<Long, List<FileTableColumnSpecWrapper>> rowNumToColumnSpec : rowNumToColumnSpecs.entrySet()) {
                List<FileTableColumnSpecWrapper> columnSpecs = rowNumToColumnSpec.getValue();
                int n = columnSpecs.size();
                FileTabularWriter writer = this.writers.get(tableName);
                Map<Long, Set<String>> rowPropIdValue = this.rowPropositionIdMap.get(tableName);
                if (rowPropIdValue != null) {
                    Set<String> rowPropIds = rowPropIdValue.get(rowNumToColumnSpec.getKey());
                    if (rowPropIds != null) {
                        Iterator<Map.Entry<String, List<Proposition>>> itr = collect.entrySet().stream().filter(me -> rowPropIds.contains(me.getKey())).iterator();
                        for (; itr.hasNext();) {
                            for (Proposition prop : itr.next().getValue()) {
                                try {
                                    for (int i = 0; i < n; i++) {
                                        FileTableColumnSpecWrapper columnSpecWrapper = columnSpecs.get(i);
                                        TabularWriterWithPool pool = columnSpecWrapper.getFileTabularWriterWithPool();
                                        pool.setTabularWriter(writer);
                                        columnSpecWrapper.getTableColumnSpec().columnValues(keyId, prop, forwardDerivations, backwardDerivations, references, this.ksCache, pool);
                                        pool.setTabularWriter(null);
                                    }
                                    writer.newRow();
                                } catch (TabularWriterException ex) {
                                    throw new QueryResultsHandlerProcessingException("Could not write row", ex);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void finish() throws QueryResultsHandlerProcessingException {
        try {
            this.idPoolDao.finish();
        } catch (PoolException ex) {
            throw new QueryResultsHandlerProcessingException("Error finishing up", ex);
        }
    }

    @Override
    public void close() throws QueryResultsHandlerCloseException {
        QueryResultsHandlerCloseException exception = null;
        exception = closeWriters(exception);
        try {
            this.idPoolDao.close();
        } catch (Exception ex) {
            if (exception != null) {
                exception.addSuppressed(ex);
            } else {
                exception = new QueryResultsHandlerCloseException(ex);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private QueryResultsHandlerCloseException closeWriters(QueryResultsHandlerCloseException exception) {
        if (this.writers != null) {
            for (FileTabularWriter writer : this.writers.values()) {
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
            FileTabularWriter writer = this.writers.get(me.getKey());
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
        for (TabularFileDestinationTableColumnEntity tableColumn : this.config.getTableColumns()) {
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
                    .map(TabularFileDestinationTableColumnEntity::getTableName)
                    .distinct()
                    .collect(Collectors.toCollection(ArrayList::new));
            String nullValue = this.config.getNullValue();
            boolean doAppend = this.query.getQueryMode() != QueryMode.REPLACE;
            if (!doAppend) {
                for (File f : outputFileDirectory.listFiles()) {
                    f.delete();
                }
            }
            for (int i = 0, n = tableNames.size(); i < n; i++) {
                String tableName = tableNames.get(i);
                File file = new File(outputFileDirectory, tableName);
                this.writers.put(tableName, new FileTabularWriter(
                        new BufferedWriter(new FileWriter(file, doAppend)),
                        this.delimiter,
                        this.config.isAlwaysQuoted() ? QuoteModel.ALWAYS : QuoteModel.WHEN_QUOTE_EMBEDDED,
                        nullValue == null ? "" : nullValue));
            }
        } catch (IOException ex) {
            throw new QueryResultsHandlerProcessingException(ex);
        }
    }

    private static FileTableColumnSpecWrapper newTableColumnSpec(
            TabularFileDestinationTableColumnEntity tableColumn,
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
