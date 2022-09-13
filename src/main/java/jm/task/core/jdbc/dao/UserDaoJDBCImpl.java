package jm.task.core.jdbc.dao;

import com.mysql.cj.x.protobuf.MysqlxPrepare;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private final String queryCreateTable = "CREATE TABLE IF NOT EXISTS `mydbtest`.`users` (\n" +
            "  `id` BIGINT(255) NOT NULL AUTO_INCREMENT,\n" +
            "  `name` VARCHAR(45) NOT NULL,\n" +
            "  `lastName` VARCHAR(45) NOT NULL,\n" +
            "  `age` TINYINT(3) NOT NULL,\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE);\n";
    private final String queryDropTable = "DROP TABLE IF EXISTS `mydbtest`.`users`\n";
    private final String queryCreate = "INSERT INTO `mydbtest`.`users` (`name`, `lastName`, `age`) VALUES (?,?,?);";
    private final String queryDeleteOnId = "DELETE FROM `mydbtest`.`users` WHERE `id` = ?";
    private final String queryGetAll = "SELECT * FROM `mydbtest`.`users`";
    private final String queryClearAll = "DELETE FROM `mydbtest`.`users`";


    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try {
            Statement statement = Util.getConnection().createStatement();
            statement.executeUpdate(queryCreateTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void dropUsersTable() {
        try {
            Statement statement = Util.getConnection().createStatement();
            statement.executeUpdate(queryDropTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {

        try {
            PreparedStatement preparedStatement = Util.getConnection().prepareStatement(queryCreate);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            System.out.println(new StringBuilder("User с именем – ").append(name).append(" добавлен в базу данных"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void removeUserById(long id) {
        try {
            PreparedStatement preparedStatement = Util.getConnection().prepareStatement(queryDeleteOnId);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> getAllUsers() {

        try {
            Statement statement = Util.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetAll);

            List<User> userList = new ArrayList<>();
            while (resultSet.next()) {

                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                String lastName = resultSet.getString("lastName");
                byte age = resultSet.getByte("age");

                User user = new User(name, lastName, age);
                user.setId(id);
                userList.add(user);
            }
            return userList;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void cleanUsersTable() {
        try {
            Statement statement = Util.getConnection().createStatement();
            statement.executeUpdate(queryClearAll);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
