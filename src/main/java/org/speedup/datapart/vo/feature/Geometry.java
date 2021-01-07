package org.speedup.datapart.vo.feature;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;


//@JsonIgnoreProperties(ignoreUnknown = true)
//@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS,include = JsonTypeInfo.As.PROPERTY,property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Point.class,name="Point"),
        @JsonSubTypes.Type(value = LineString.class,name ="LineString"),
        @JsonSubTypes.Type(value = Polygon.class,name ="Polygon"),
        @JsonSubTypes.Type(value = MultiPoint.class,name = "MultiPoint"),
        @JsonSubTypes.Type(value = MultiLineString.class,name = "MultiLineString"),
        @JsonSubTypes.Type(value = MultiPolygon.class,name = "MultiPolygon")
})
@JsonDeserialize()
public class Geometry<T> {

    private String type;

    @JsonProperty(value = "coordinates")
    @SerializedName(value = "coordinates")
    private T positions;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public T getPositions() {
        return positions;
    }

    public void setPositions(T positions) {
        this.positions = positions;
    }

    public  BBOX getBBOX(){
        return null;
    }

    @Override
    public String toString() {
       return  new Gson().toJson(this);
    }
}
