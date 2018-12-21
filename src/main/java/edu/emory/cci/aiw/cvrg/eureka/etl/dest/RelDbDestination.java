package edu.emory.cci.aiw.cvrg.eureka.etl.dest;

import edu.emory.cci.aiw.cvrg.eureka.etl.config.EtlProperties;
import edu.emory.cci.aiw.cvrg.eureka.etl.dao.ETLIdPoolDao;
import edu.emory.cci.aiw.cvrg.eureka.etl.entity.RelDbDestinationEntity;
import java.util.List;
import javax.inject.Provider;
import org.protempa.DataSource;
import org.protempa.KnowledgeSource;
import org.protempa.ProtempaEventListener;
import org.protempa.dest.AbstractDestination;
import org.protempa.dest.QueryResultsHandler;
import org.protempa.dest.QueryResultsHandlerInitException;
import org.protempa.query.Query;

/**
 *
 * @author Andrew Post
 */
public class RelDbDestination extends AbstractDestination {
    
    private final RelDbDestinationEntity relDbDestinationEntity;
    private final EtlProperties etlProperties;
    private final Provider<ETLIdPoolDao> idPoolDaoProvider;

    public RelDbDestination(EtlProperties inEtlProperties, RelDbDestinationEntity inRelDbDestinationEntity, Provider<ETLIdPoolDao> inIdPoolDaoProvider) {
        assert inRelDbDestinationEntity != null : "inRelDbDestinationEntity cannot be null";
        this.relDbDestinationEntity = inRelDbDestinationEntity;
        this.etlProperties = inEtlProperties;
        this.idPoolDaoProvider = inIdPoolDaoProvider;
    }
    
    @Override
    public QueryResultsHandler getQueryResultsHandler(Query query, DataSource dataSource, KnowledgeSource knowledgeSource, List<? extends ProtempaEventListener> eventListeners) throws QueryResultsHandlerInitException {
        return new RelDbQueryResultsHandler(query, this.relDbDestinationEntity, this.etlProperties, knowledgeSource, this.idPoolDaoProvider);
    }
    
}
