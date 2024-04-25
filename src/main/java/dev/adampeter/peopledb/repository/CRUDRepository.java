package dev.adampeter.peopledb.repository;

import dev.adampeter.peopledb.annotation.Id;
import dev.adampeter.peopledb.annotation.MultiSQL;
import dev.adampeter.peopledb.annotation.SQL;
import dev.adampeter.peopledb.exception.UnableToSaveException;
import dev.adampeter.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class CRUDRepository<T> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);

            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();

            while (rs.next()) {
                long id = rs.getLong(1);
                setIdByAnnotation(entity, id);
                postSave(entity, id);
                // System.out.println(entity);
            }
            // System.out.printf("Records saved: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save entity: " + entity);
        }

        return entity;
    }

    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Optional.ofNullable(entity); // entity might still be null if we don't get throught the try-catch
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            ResultSet rs = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql)).executeQuery();
            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return entities;
    }

    public long count() {
        long count = 0L;
        try {
            ResultSet rs = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountAllSql)).executeQuery();
            while (rs.next()) {
                count = rs.getLong("COUNT"); // COUNT(*) was aliased in sql
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteOneSql));
            ps.setLong(1, getIdByAnnotation(entity));
            int affectedRecordCount = ps.executeUpdate();
            System.out.printf("Deleted records: %d%n", affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void delete(T... entities) { // entity[] people
        try {
            Statement st = connection.createStatement();
            String ids = Arrays.stream(entities).mapToLong(this::getIdByAnnotation).mapToObj(Long::toString).collect(Collectors.joining(","));

            int affectedRecordCount = st.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteManySql).replace(":ids", ids));
            System.out.printf("Deleted records: %d%n", affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateOneSql));
            mapForUpdate(entity, ps);
            ps.setLong(5, getIdByAnnotation(entity));
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        // to process MultiSQL streams
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .flatMap(msql -> Arrays.stream(msql.value())); // flatMap to flatten a Stream<Stream<T>> into a Stream<T>

        // to process single SQL streams
        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(a -> a.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter); // using Suppliers / method references help us - java doesn't have to evaluate the value if it's not used
    }

    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true); // overwriting the private of the field
                    Long id = null;
                    try {
                        id = (long) f.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated field found."));
    }

    private void setIdByAnnotation(T entity, Long id) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set ID field value.");
                    }
                });
    }

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    protected void postSave(T entity, long id) {
    }

    protected String getSaveSql() {
        throw new RuntimeException("SQL not defined.");
    }

    /**
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected String getFindByIdSql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getFindAllSql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getCountAllSql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getDeleteOneSql() {
        throw new RuntimeException("SQL not defined.");
    }

    /**
     * @return Should return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it
     */
    protected String getDeleteManySql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getUpdateOneSql() {
        throw new RuntimeException("SQL not defined.");
    }
}
