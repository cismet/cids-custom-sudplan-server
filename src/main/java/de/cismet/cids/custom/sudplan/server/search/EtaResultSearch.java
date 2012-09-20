/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.search;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.types.MetaClass;
import Sirius.server.middleware.types.MetaObject;
import Sirius.server.middleware.types.MetaObjectNode;
import Sirius.server.middleware.types.Node;
import Sirius.server.search.CidsServerSearch;

import org.apache.log4j.Logger;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.cismet.cids.custom.sudplan.commons.SudplanConcurrency;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class EtaResultSearch extends CidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final transient Logger LOG = Logger.getLogger(EtaResultSearch.class);
    private static final String STMT_TEST_SUDPLAN_SYSTEM = "SELECT e.id, r.id, s.id "
                + "FROM linz_eta_result e, run r, linz_swmm_run s "
                + "LIMIT 1";
    private static final String STMT_ETA_RESULT_TEMPLATE =
        "SELECT DISTINCT ETA_RESULT.eta_scenario_id FROM \"public\".linz_eta_result ETA_RESULT "
                + "RIGHT OUTER JOIN \"public\".run AS RUN ON ETA_RESULT.eta_scenario_id = RUN.id "
                + "JOIN \"public\".linz_swmm_run AS SWMM_RUN ON SWMM_RUN.swmm_scenario = ETA_RESULT.swmm_scenario_id "
                + "AND SWMM_RUN.swmm_project_reference = ";
    private static final String STMT_ETA_HYD_CLAUSE = " AND ETA_RESULT.eta_hyd_actual >= ETA_RESULT.eta_hyd_required ";
    private static final String STMT_ETA_SED_CLAUSE = " AND ETA_RESULT.eta_sed_actual >= ETA_RESULT.eta_sed_required ";
    public static final int NONE = 0;
    public static final int ETA_HYD = 1;
    public static final int ETA_SED = 2;
    public static final String DOMAIN = "SUDPLAN";

    //~ Instance fields --------------------------------------------------------

    private final String etaResultStatement;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CsosWithoutOverflowSearch object.
     *
     * @param  swmmProjectId  swmmRunId DOCUMENT ME!
     * @param  etaParameter   DOCUMENT ME!
     */
    public EtaResultSearch(final int swmmProjectId, final int etaParameter) {
        switch (etaParameter) {
            case ETA_HYD: {
                etaResultStatement = STMT_ETA_RESULT_TEMPLATE
                            + swmmProjectId
                            + STMT_ETA_HYD_CLAUSE;
                break;
            }
            case ETA_SED: {
                etaResultStatement = STMT_ETA_RESULT_TEMPLATE
                            + swmmProjectId
                            + STMT_ETA_SED_CLAUSE;
                break;
            }

            case ETA_HYD
                    + ETA_SED: {
                etaResultStatement = STMT_ETA_RESULT_TEMPLATE
                            + swmmProjectId
                            + STMT_ETA_HYD_CLAUSE
                            + STMT_ETA_SED_CLAUSE;
                break;
            }

            default: {
                etaResultStatement = STMT_ETA_RESULT_TEMPLATE
                            + swmmProjectId;
            }
        }
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection performServerSearch() {
        final ExecutorService searcher = Executors.newCachedThreadPool(
                SudplanConcurrency.createThreadFactory("eta-result-search")); // NOI18N

        final Map map = getActiveLoaclServers();
        final ArrayList<EtaResultSearch.EtaResultFetcher> fetchers = new ArrayList<EtaResultSearch.EtaResultFetcher>(
                map.size());
        for (final Object o : map.keySet()) {
            final String domain = (String)o;
            final MetaService ms = (MetaService)map.get(domain);

            final EtaResultSearch.EtaResultFetcher fetcher = new EtaResultSearch.EtaResultFetcher(ms, domain);
            // keep track of the fetchers since we want to know the results afterwards
            fetchers.add(fetcher);
            // we don't need the future since we'return going to shutdown the service anyway
            searcher.submit(fetcher);
        }

        searcher.shutdown();
        // we wait for the searcher to shutdown and bail out returning null if there is any abnormal termination
        try {
            if (!searcher.awaitTermination(2, TimeUnit.MINUTES)) {
                LOG.error("the searches did not finish within 2 minutes, search unsuccessful, returning null"); // NOI18N

                return null;
            }
        } catch (final InterruptedException ex) {
            LOG.error("waiting for the searcher pool was interrupted, search unsuccessful, returning null"); // NOI18N

            return null;
        }

        final ArrayList<Node> result = new ArrayList<Node>();
        for (final EtaResultSearch.EtaResultFetcher fetcher : fetchers) {
            if (fetcher.getException() == null) {
                result.addAll(fetcher.getResult());
            } else {
                LOG.error(
                    "at least one EtaResultFetcher terminated abnormally, search unsuccessful, returning", // NOI18N
                    fetcher.getException());

                return null;
            }
        }

        return result;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @version  $Revision$, $Date$
     */
    private final class EtaResultFetcher implements Runnable {

        //~ Instance fields ----------------------------------------------------

        private final transient MetaService ms;
        private final transient String domain;
        private final transient List<Node> result;
        private transient Exception exception;

        //~ Constructors -------------------------------------------------------

        /**
         * Creates a new CsoFetcher object.
         *
         * @param  ms      DOCUMENT ME!
         * @param  domain  DOCUMENT ME!
         */
        EtaResultFetcher(final MetaService ms, final String domain) {
            this.ms = ms;
            this.domain = domain;
            this.exception = null;
            this.result = new ArrayList<Node>();
        }

        //~ Methods ------------------------------------------------------------

        /**
         * DOCUMENT ME!
         *
         * @return  DOCUMENT ME!
         */
        List<Node> getResult() {
            return result;
        }

        /**
         * DOCUMENT ME!
         *
         * @return  DOCUMENT ME!
         */
        Exception getException() {
            return exception;
        }

        @Override
        public void run() {
            // test if the server is a sudplan compatible server and has at least one entry in every relevant table
            try {
                final Object o = ms.performCustomSearch(STMT_TEST_SUDPLAN_SYSTEM);
            } catch (RemoteException ex) {
                // we caught an exception so we ignore this server
                LOG.info("EtaResultFetcher: ignoring server since test for sudplan system failed: " + domain, ex); // NOI18N

                return;
            }

            final int csoClassId;
            try {
                final MetaClass metaClass = ms.getClassByTableName(getUser(), "run"); // NOI18N
                csoClassId = metaClass.getID();
            } catch (final Exception ex) {
                LOG.error("cannot fetch RUN metaclass (run)", ex);                    // NOI18N
                exception = ex;

                return;
            }

            // now search for the runs
            final int[] objectIds;
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(etaResultStatement);
                }
                final ArrayList<ArrayList> results = ms.performCustomSearch(etaResultStatement);
                objectIds = new int[results.size()];

                for (int i = 0; i < results.size(); ++i) {
                    final ArrayList al = results.get(i);
                    objectIds[i] = (Integer)al.get(0);
                }
            } catch (final Exception e) {
                LOG.error("cannot fetch RUNs", e); // NOI18N
                exception = e;

                return;
            }

            // finally build cidsbeans from the results
            try {
                for (final int objectId : objectIds) {
                    final MetaObject object = ms.getMetaObject(getUser(), objectId, csoClassId);
                    if (object != null) {
                        final Node node = new MetaObjectNode(object.getBean());
                        result.add(node);
                    } else {
                        LOG.warn("no run found for id " + objectId);
                    }
                }
            } catch (final Exception e) {
                LOG.error("cannot create metaobjects from found results", e); // NOI18N
                exception = e;
            }
        }
    }
}
