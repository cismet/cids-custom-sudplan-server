/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.trigger;

import it.geosolutions.geoserver.rest.encoder.utils.XmlElement;

/**
 * DOCUMENT ME!
 *
 * @author   bfriedrich
 * @version  $Revision$, $Date$
 */
public class GSAttributeEncoder extends XmlElement {

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new GSAttributeEncoder object.
     */
    public GSAttributeEncoder() {
        super("attribute");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param  name   DOCUMENT ME!
     * @param  value  DOCUMENT ME!
     */
    public void addEntry(final String name, final String value) {
        super.add(name, value);
    }
}
