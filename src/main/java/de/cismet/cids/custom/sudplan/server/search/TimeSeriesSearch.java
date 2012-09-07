/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.search;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.search.CidsServerSearch;

import java.rmi.RemoteException;

import java.text.MessageFormat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TimeSeriesSearch is utilized for finding TimeSeries records having the specified name. It searches in every available
 * domain as the current storage for timeseries is centralised.
 *
 * @author   Benjamin Friedrich (benjamin.friedrich@cismet.de)
 * @author   Martin Scholl (martin.scholl@cismet.de)
 * @version  1.1, 07.09.2012
 */
public final class TimeSeriesSearch extends CidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final String QUERY = "select id from timeseries where name = ''{0}''"; // NOI18N

    //~ Instance fields --------------------------------------------------------

    private final String name;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new WkSearchByStations object.
     *
     * @param   name  from DOCUMENT ME!
     *
     * @throws  NullPointerException      DOCUMENT ME!
     * @throws  IllegalArgumentException  DOCUMENT ME!
     */
    public TimeSeriesSearch(final String name) {
        if (name == null) {
            throw new NullPointerException("Name must not be null"); // NOI18N
        }

        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name must not be empty"); // NOI18N
        }

        this.name = name;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection performServerSearch() {
        final Map<String, MetaService> mss = getActiveLoaclServers();

        try {
            final String query = MessageFormat.format(QUERY, this.name);

            if (getLog().isDebugEnabled()) {
                getLog().debug("query: " + query); // NOI18N
            }

            final List<Map<String, List<? extends List>>> lists = new ArrayList<Map<String, List<? extends List>>>(
                    mss.size());

            for (final String domain : mss.keySet()) {
                final MetaService ms = mss.get(domain);

                if (ms != null) {
                    final ArrayList<ArrayList> timeseries = ms.performCustomSearch(query);

                    if ((timeseries != null) && !timeseries.isEmpty()) {
                        if (lists.isEmpty()) {
                            lists.add(new HashMap<String, List<? extends List>>());
                        }
                        lists.get(0).put(domain, timeseries);
                    }
                }
            }

            return lists;
        } catch (final RemoteException ex) {
            final String message = "cannot perform time series search"; // NOI18N
            getLog().error(message, ex);

            throw new IllegalStateException(message, ex); // NOI18N
        }
    }
}
