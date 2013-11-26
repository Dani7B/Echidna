package it.cybion.communityInteraction.landingArea.util;

/**
* @author Daniele Morgantini */
public class HadoopException extends Exception {

        private static final long serialVersionUID = 1L;

        public HadoopException(final String message) {
                super(message);
        }

        public HadoopException(final String message, final Exception e) {
                super(message,e);
        }
}