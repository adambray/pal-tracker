package io.pivotal.pal.tracker;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.Date;
import java.util.List;

public class JdbcTimeEntryRepository implements TimeEntryRepository {
    private final JdbcTemplate template;

    public JdbcTimeEntryRepository(DataSource dataSource) {
        template = new JdbcTemplate();
        template.setDataSource(dataSource);
    }

    @Override
    public TimeEntry create(TimeEntry timeEntry) {
        String insertSql = "INSERT INTO time_entries(" +
                "project_id," +
                "user_id," +
                "date," +
                "hours" +
                ") " +
                "VALUES(?, ?, ?, ?) RETURNING ID";

        long id = template.queryForObject(insertSql,
                new Object[]{
                        timeEntry.getProjectId(),
                        timeEntry.getUserId(),
                        timeEntry.getDate(),
                        timeEntry.getHours()
                },
                Integer.class);

        return new TimeEntry(id, timeEntry.getProjectId(), timeEntry.getUserId(), timeEntry.getDate(), timeEntry.getHours());
    }

    @Override
    public TimeEntry find(long id) {
        String findSql = "SELECT * FROM time_entries where id = ?";

        TimeEntry timeEntry = template.query(findSql, new Object[]{id}, extractor);
        return timeEntry;
    }

    @Override
    public List<TimeEntry> list() {
        String findSql = "SELECT * FROM time_entries ORDER BY id";

        return template.query(findSql, (rs, rowNum) ->
                new TimeEntry(
                        rs.getLong("id"),
                        rs.getLong("project_id"),
                        rs.getLong("user_id"),
                        rs.getDate("date").toLocalDate(),
                        rs.getInt("hours")
                ));
    }

    @Override
    public TimeEntry update(long id, TimeEntry timeEntry) {
        template.update("UPDATE time_entries " +
                        "SET project_id = ?, user_id = ?, date = ?,  hours = ? " +
                        "WHERE id = ?",
                timeEntry.getProjectId(),
                timeEntry.getUserId(),
                Date.valueOf(timeEntry.getDate()),
                timeEntry.getHours(),
                id);

        return find(id);
    }

    @Override
    public void delete(long id) {
        template.update("DELETE FROM time_entries WHERE id = ?", id);
    }

    private final RowMapper<TimeEntry> mapper = (rs, rowNum) -> new TimeEntry(
            rs.getLong("id"),
            rs.getLong("project_id"),
            rs.getLong("user_id"),
            rs.getDate("date").toLocalDate(),
            rs.getInt("hours")
    );

    private final ResultSetExtractor<TimeEntry> extractor =
            (rs) -> rs.next() ? mapper.mapRow(rs, 1) : null;
}
