/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.search;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class CsosWithoutOverflowSearch extends CsoByOverflowSearch {

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CsosWithoutOverflowSearch object.
     *
     * @param  swmmRunId  DOCUMENT ME!
     */
    public CsosWithoutOverflowSearch(final int swmmRunId) {
        super(swmmRunId, 0.0f);
        this.searchName = "csos-without-overflow-search";
    }
}
