package dev.adampeter.peopledb.repository;

import dev.adampeter.peopledb.model.Address;
import dev.adampeter.peopledb.annotation.SQL;
import dev.adampeter.peopledb.model.CrudOperation;
import dev.adampeter.peopledb.model.Region;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CRUDRepository<Address> {
    public AddressRepository(Connection connection) {
        super(connection);
    }

    @Override
    @SQL(operationType = CrudOperation.SAVE, value = """
            INSERT INTO ADDRESSES(STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """)
    void mapForSave(Address entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.streetAddress());
        ps.setString(2, entity.address2());
        ps.setString(3, entity.city());
        ps.setString(4, entity.state());
        ps.setString(5, entity.postcode());
        ps.setString(6, entity.county());
        ps.setString(7, entity.region().toString());
        ps.setString(8, entity.country());
    }

    @Override
    void mapForUpdate(Address entity, PreparedStatement ps) throws SQLException {

    }

    @Override
    @SQL(operationType = CrudOperation.FIND_BY_ID, value = """
            SELECT ID, STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTRY, COUNTY, REGION
            FROM ADDRESSES
            WHERE ID = ?
            """)
    Address extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long id = rs.getLong("ID");
        String streetAddress = rs.getString("STREET_ADDRESS");
        String address2 = rs.getString("ADDRESS2");
        String city = rs.getString("CITY");
        String state = rs.getString("STATE");
        String postcode = rs.getString("POSTCODE");
        String country = rs.getString("COUNTRY");
        String county = rs.getString("COUNTY");
        Region region = Region.valueOf(rs.getString("REGION").toUpperCase());

        return new Address(id, streetAddress, address2, city, state, postcode, country, county, region);
    }
}
