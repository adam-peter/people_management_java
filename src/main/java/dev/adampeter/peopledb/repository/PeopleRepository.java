package dev.adampeter.peopledb.repository;

import dev.adampeter.peopledb.annotation.SQL;
import dev.adampeter.peopledb.model.Address;
import dev.adampeter.peopledb.model.CrudOperation;
import dev.adampeter.peopledb.model.Person;
import dev.adampeter.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

public class PeopleRepository extends CRUDRepository<Person> {
    private static final String SAVE_PERSON_SQL = """
                INSERT INTO PEOPLE
                (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BUSINESS_ADDRESS, SPOUSE, PARENT)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    private static final String FIND_BY_ID_SQL = """
                SELECT
                PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME, PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL,
                CHILD.ID AS CHILD_ID, CHILD.FIRST_NAME AS CHILD_FIRST_NAME, CHILD.LAST_NAME AS CHILD_LAST_NAME, CHILD.DOB AS CHILD_DOB, CHILD.SALARY AS CHILD_SALARY, CHILD.EMAIL AS CHILD_EMAIL,
                HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY, HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTRY AS HOME_COUNTRY, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION,
                BUSINESS.ID AS BUSINESS_ID, BUSINESS.STREET_ADDRESS AS BUSINESS_STREET_ADDRESS, BUSINESS.ADDRESS2 AS BUSINESS_ADDRESS2, BUSINESS.CITY AS BUSINESS_CITY, BUSINESS.STATE AS BUSINESS_STATE, BUSINESS.POSTCODE AS BUSINESS_POSTCODE, BUSINESS.COUNTRY AS BUSINESS_COUNTRY, BUSINESS.COUNTY AS BUSINESS_COUNTY, BUSINESS.REGION AS BUSINESS_REGION,
                FROM PEOPLE AS PARENT
                LEFT OUTER JOIN PEOPLE AS CHILD ON PARENT.ID = CHILD.PARENT
                LEFT OUTER JOIN ADDRESSES AS HOME ON PARENT.HOME_ADDRESS = HOME.ID
                LEFT OUTER JOIN ADDRESSES AS BUSINESS ON PARENT.BUSINESS_ADDRESS = BUSINESS.ID
                WHERE PARENT.ID = ?;
            """;
    private static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE FETCH FIRST 100 ROWS ONLY";
    private static final String COUNT_ALL_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    private static final String DELETE_ONE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    private static final String DELETE_MANY_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    private static final String UPDATE_ONE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID =?";

    private AddressRepository addressRepository = null;

    public PeopleRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }

    @Override
    // @MultiSQL(..., ...)
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = COUNT_ALL_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_ONE_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_MANY_SQL, operationType = CrudOperation.DELETE_MANY)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        Person parent = extractPerson(rs, "PARENT_");

        Address homeAddress = extractAddress(rs, "HOME_");
        Address businessAddress = extractAddress(rs, "BUSINESS_");
        parent.setHomeAddress(homeAddress);
        parent.setBusinessAddress(businessAddress);
//        Person spouse = extractSpouse(rs);
//        parent.setSpouse(spouse);

        do {
            if (!rs.wasNull()) {
                Person foundChild = extractPerson(rs, "CHILD_");
                parent.addChild(foundChild);
            }
        } while (rs.next());
        return parent;
    }

    private static Person extractPerson(ResultSet rs, String aliasPrefix) throws SQLException {
        Long personId = rs.getLong(aliasPrefix + "ID");
        String personFirstName = rs.getString(aliasPrefix + "FIRST_NAME");
        String personLastName = rs.getString(aliasPrefix + "LAST_NAME");
        ZonedDateTime personDob = ZonedDateTime.of(rs.getTimestamp(aliasPrefix + "DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal(aliasPrefix + "SALARY");
        Person person = new Person(personId, personFirstName, personLastName, personDob, salary);
        return person;
    }

    private Address extractAddress(ResultSet rs, String aliasPrefix) throws SQLException {
        if (rs.getObject(aliasPrefix + "ID") == null) {
            return null;
        }

        // long addressId = getValueByAlias("HOME_ID", rs, Long.class); - alternative
        long addressId = rs.getLong(aliasPrefix + "ID");
        String streetAddress = rs.getString(aliasPrefix + "STREET_ADDRESS");
        String address2 = rs.getString(aliasPrefix + "ADDRESS2");
        String city = rs.getString(aliasPrefix + "CITY");
        String state = rs.getString(aliasPrefix + "STATE");
        String postcode = rs.getString(aliasPrefix + "POSTCODE");
        String country = rs.getString(aliasPrefix + "COUNTRY");
        String county = rs.getString(aliasPrefix + "COUNTY");
        Region region = Region.valueOf(rs.getString(aliasPrefix + "REGION").toUpperCase());

        return new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
    }

    private Person extractSpouse(ResultSet rs) throws SQLException {
        if (rs.getObject("SPOUSE") == null) {
            return null;
        }

        long spouseId = rs.getLong("SPOUSE_ID");
        String spouseFirstName = rs.getString("SPOUSE_FIRST_NAME");
        String spouseLastName = rs.getString("SPOUSE_LAST_NAME");
        ZonedDateTime spouseDob = ZonedDateTime.of(rs.getTimestamp("SPOUSE_DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal spouseSalary = rs.getBigDecimal("SPOUSE_SALARY");

        return new Person(spouseId, spouseFirstName, spouseLastName, spouseDob, spouseSalary);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName()); // not zero-based indexing
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());

        setAddress(ps, entity.getHomeAddress(), 6);
        setAddress(ps, entity.getBusinessAddress(), 7);
        setSpouse(entity, ps);
        associateChildWithPerson(entity, ps);
    }

    private void setAddress(PreparedStatement ps, Optional<Address> address, int parameterIndex) throws SQLException {
        if (address.isPresent()) {
            Address homeAddress = null;
            homeAddress = addressRepository.save(address.get());
            ps.setLong(parameterIndex, homeAddress.id());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    private void setSpouse(Person entity, PreparedStatement ps) throws SQLException {
        if (entity.getSpouse().isPresent()) {
            Person spouse = save(entity.getSpouse().get());
            ps.setLong(8, spouse.getId());
        } else {
            ps.setObject(8, null);
        }
    }

    private static void associateChildWithPerson(Person entity, PreparedStatement ps) throws SQLException {
        Optional<Person> parent = entity.getParent();
        if (parent.isPresent()) {
            ps.setLong(9, parent.get().getId());
        } else {
            ps.setObject(9, null);
        }
    }

    @Override
    protected void postSave(Person entity, long id) {
        entity.getChildren().stream()
                .forEach(this::save);
    }

    @Override
    @SQL(value = UPDATE_ONE_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }
}
