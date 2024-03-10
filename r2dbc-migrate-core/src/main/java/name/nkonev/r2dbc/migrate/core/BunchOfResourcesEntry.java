package name.nkonev.r2dbc.migrate.core;

import java.util.ArrayList;
import java.util.List;

public class BunchOfResourcesEntry {

    private List<String> resourcesPaths = new ArrayList<>();

    private BunchOfResourcesType type = BunchOfResourcesType.CONVENTIONALLY_NAMED_FILES;

    private Long version; // only for JUST_FILES
    private String description; // only for JUST_FILES
    private Boolean splitByLine; // only for JUST_FILES
    private Boolean transactional; // only for JUST_FILES
    private Boolean premigration; // only for JUST_FILES


    public BunchOfResourcesEntry() {
    }

    public List<String> getResourcesPaths() {
        return resourcesPaths;
    }

    public void setResourcesPaths(List<String> resourcesPaths) {
        this.resourcesPaths = resourcesPaths;
    }

    public BunchOfResourcesType getType() {
        return type;
    }

    public void setType(BunchOfResourcesType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "BunchOfResourcesEntry{" +
            "resourcesPaths=" + resourcesPaths +
            ", type=" + type +
            '}';
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getSplitByLine() {
        return splitByLine;
    }

    public void setSplitByLine(Boolean splitByLine) {
        this.splitByLine = splitByLine;
    }

    public Boolean getTransactional() {
        return transactional;
    }

    public void setTransactional(Boolean transactional) {
        this.transactional = transactional;
    }

    public Boolean getPremigration() {
        return premigration;
    }

    public void setPremigration(Boolean premigration) {
        this.premigration = premigration;
    }

    public static BunchOfResourcesEntry ofConventionallyNamedFiles(List<String> resourcesPaths) {
        var e = new BunchOfResourcesEntry();
        e.setType(BunchOfResourcesType.CONVENTIONALLY_NAMED_FILES);
        e.setResourcesPaths(resourcesPaths);
        return e;
    }
}