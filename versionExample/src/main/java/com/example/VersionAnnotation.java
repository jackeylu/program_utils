package com.example;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This class is about package attribute that captures
 * version info of project that was compiled.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)
public @interface VersionAnnotation {
    /**
     * Get the Project version
     * @return the version string "1.1"
     */
    String version();

    /**
     * Get the subversion revision.
     * @return the revision number as a string (eg. "100755")
     */
    String revision();

    /**
     * Get the branch from which this was compiled.
     * @return The branch name, e.g. "trunk"
     */
    String branch();

    /**
     * Get the username that compiled Project.
     */
    String user();

    /**
     * Get the date when Project was compiled.
     * @return the date in unix 'date' format
     */
    String date();

    /**
     * Get the url for the subversion repository.
     */
    String url();

    /**
     * Get a checksum of the source files from which
     * Project was compiled.
     * @return a string that uniquely identifies the source
     **/
    String srcChecksum();
}
