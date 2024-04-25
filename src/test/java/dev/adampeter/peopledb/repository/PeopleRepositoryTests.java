package dev.adampeter.peopledb.repository;

import dev.adampeter.peopledb.model.Address;
import dev.adampeter.peopledb.model.Person;
import dev.adampeter.peopledb.model.Region;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private static Connection connection;
    private static PeopleRepository repo;

    @BeforeAll
    static void setUp() throws SQLException { // don't use try-catch in tests  - test fails on exception
        connection = DriverManager.getConnection("jdbc:h2:/home/adam/Desktop/peopletest;TRACE_LEVEL_SYSTEM_OUT=0");
        connection.setAutoCommit(false); // stops commiting the changes to the db

        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void realAfterEach() throws SQLException {
        connection.rollback();
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Fischer", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bobby);

        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canSavePersonWithHomeAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setHomeAddress(address);
        Person savedPerson = repo.save(john);

//        connection.commit();
        assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithBusinessAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setBusinessAddress(address);
        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);
    }

    @Disabled
    @Test
    public void canSavePersonWithSpouse() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person june = new Person("June", "Smith", ZonedDateTime.of(1990, 6, 7, 10, 0, 0, 0, ZoneId.of("-6")));
        john.setSpouse(june);
        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getSpouse().get().getId()).isEqualTo(june.getId());
    }

    @Test
    public void canSavePersonWithChildren() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        john.addChild(new Person("Jimmy", "Smith", ZonedDateTime.of(2010, 1, 1, 1, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Johny", "Smith", ZonedDateTime.of(2012, 3, 1, 1, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(2014, 5, 1, 1, 0, 0, 0, ZoneId.of("-6"))));

        Person savedPerson = repo.save(john);
        savedPerson.getChildren().stream()
                .map(Person::getId)
                .forEach(id -> assertThat(id).isGreaterThan(0L));
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("test", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        Person foundPerson = repo.findById(savedPerson.getId()).get(); // get should always work in tests - if it fails, the test fails, which is good

        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonByIdWithHomeAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getHomeAddress().get().state()).isEqualTo(savedPerson.getHomeAddress().get().state());
    }

    @Test
    public void canFindPersonByIdWithBusinessAddress() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala", "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setBusinessAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo(savedPerson.getBusinessAddress().get().state());
    }

    @Disabled
    @Test
    public void canFindPersonByIdWithSpouse() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person june = new Person("June", "Smith", ZonedDateTime.of(1990, 6, 7, 10, 0, 0, 0, ZoneId.of("-6")));

        john.setSpouse(june);
        Person p1 = repo.save(john);
        Person p2 = repo.save(june);

        Person foundJohn = repo.findById(p1.getId()).get();
        Person foundJune = repo.findById(p2.getId()).get();
        assertThat(foundJohn.getSpouse().get().getFirstName()).isEqualTo(foundJune.getFirstName());
    }

    @Test
    public void canFindPersonByIdWithChildren() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        john.addChild(new Person("Jimmy", "Smith", ZonedDateTime.of(2010, 1, 1, 1, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Johny", "Smith", ZonedDateTime.of(2012, 3, 1, 1, 0, 0, 0, ZoneId.of("-6"))));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(2014, 5, 1, 1, 0, 0, 0, ZoneId.of("-6"))));

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getChildren().stream()
                .map(Person::getFirstName)
                .collect(Collectors.toSet())
        ).contains("Jimmy", "Johny", "Jenny");
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canFindAll() {
        repo.save(new Person("test1", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test2", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test3", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test4", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test5", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test6", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test7", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test8", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));

        List<Person> people = repo.findAll();
        assertThat(people.size()).isGreaterThanOrEqualTo(10);
    }

    @Disabled
    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("test1", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("test2", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Disabled
    @Test
    public void canDeleteOnePerson() {
        Person savedPerson = repo.save(new Person("test", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = repo.save(new Person("test1", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        Person p2 = repo.save(new Person("test2", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        repo.delete(p1, p2);
    }

    @Disabled
    @Test
    public void loadData() throws IOException, SQLException {
        Files.lines(Path.of("/home/adam/Desktop/Hr5m.csv"))
                .skip(1) // skip header row
//                .limit(100)
                .map(l -> l.split(","))
                .map(arr -> {
                    LocalDate dob = LocalDate.parse(arr[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    LocalTime tob = LocalTime.parse(arr[11], DateTimeFormatter.ofPattern("hh:mm:ss a"));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));

                    Person person = new Person(arr[2], arr[4], zdtob);
                    person.setSalary(new BigDecimal(arr[25]));
                    person.setEmail(arr[6]);
                    return person;
                })
                .forEach(repo::save); // p -> repo.save(p)

//        connection.commit();
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("test", "testtest", ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("+0"))));
        Person p1 = repo.findById(savedPerson.getId()).get(); // salary of 0

        savedPerson.setSalary(new BigDecimal("73000.28"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get(); // salary of 73000.28
        assertThat(p2.getSalary()).isNotEqualByComparingTo(p1.getSalary());
    }
}

