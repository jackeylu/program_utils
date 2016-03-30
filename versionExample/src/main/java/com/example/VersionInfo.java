package com.example;

/**
 * Created by jackeylv on 2016/1/13.
 */
public class VersionInfo {

    private static Package myPackage;
    private static VersionAnnotation version;

    static {
        myPackage = VersionAnnotation.class.getPackage();
        version = myPackage.getAnnotation(VersionAnnotation.class);
    }

    /**
     * Get the meta-data for the sink package.
     * @return
     */
    static Package getPackage() {
        return myPackage;
    }

    /**
     * Get the sink version.
     * @return the sink version string, eg. "1.1"
     */
    public static String getVersion() {
        return version != null ? version.version() : "Unknown";
    }

    /**
     * Get the subversion revision number for the root directory
     * @return the revision number, eg. "100755"
     */
    public static String getRevision() {
        if(version != null
                && version.revision() != null
                && !version.revision().isEmpty()){
            return version.revision();
        }
        return "Unknown";
    }

    /**
     * Get the branch on which this originated.
     * @return The branch name, e.g. "trunk" or "branches/branch-1.1"
     */
    public static String getBranch() {
        return version != null ? version.branch() : "Unknown";
    }

    /**
     * The date that sink was compiled.
     * @return the compilation date in unix date format
     */
    public static String getDate() {
        return version != null ? version.date() : "Unknown";
    }

    /**
     * The user that compiled sink.
     * @return the username of the user
     */
    public static String getUser() {
        return version != null ? version.user() : "Unknown";
    }

    /**
     * Get the subversion URL for the root sink directory.
     */
    public static String getUrl() {
        return version != null ? version.url() : "Unknown";
    }

    /**
     * Get the checksum of the source files from which sink was
     * built.
     **/
    public static String getSrcChecksum() {
        return version != null ? version.srcChecksum() : "Unknown";
    }

    /**
     * Returns the build version info which includes version,
     * revision, user, date and source checksum
     */
    public static String getBuildVersion(){
        return VersionInfo.getVersion() +
                " from " + VersionInfo.getRevision() +
                " by " + VersionInfo.getUser() +
                " on " + VersionInfo.getDate() +
                " source checksum " + VersionInfo.getSrcChecksum();
    }

    public static void main(String[] args) {
        System.out.println("VersionExample " + getVersion());
        System.out.println("Source code repository url: " + getUrl());
        System.out.println("Revision: " + getRevision());
        System.out.println("Compiled by " + getUser() + " on " + getDate());
        System.out.println("From source with checksum " + getSrcChecksum());
        System.out.println("VersionInfo.main with build version " + getBuildVersion());
        System.out.println("VersionInfo.main getPackage() = " + getPackage());
    }
}
