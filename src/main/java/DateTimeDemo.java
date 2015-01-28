import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author serch
 */
public class DateTimeDemo {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        LocalDate hoy = LocalDate.now();
        LocalDate proxMiercoles = hoy.with(TemporalAdjusters.next(DayOfWeek.WEDNESDAY));
        LocalDate primeroProxMes = hoy.with(TemporalAdjusters.firstDayOfNextMonth());
        LocalDate ultimoVienresProximoMes = hoy.with(TemporalAdjusters.lastInMonth(DayOfWeek.FRIDAY));
        
        LocalDate enUnaSemana = hoy.plus(7, ChronoUnit.DAYS);
        LocalDate enDosMeses = hoy.plus(2, ChronoUnit.MONTHS);
        LocalDate haceUnAnio = hoy.minus(1, ChronoUnit.YEARS);
        boolean esBisiesto = hoy.isLeapYear();
        System.out.println("hoy: "+ hoy );
        System.out.println("hoy + dos semanas: "+hoy.plusWeeks(2));
        System.out.println("El próximo miercoles: "+hoy.with(TemporalAdjusters.previous(DayOfWeek.WEDNESDAY)));
        ZonedDateTime zdt = ZonedDateTime.now();
        ZoneId id = ZoneId.of("Europe/Paris");
        ZonedDateTime paris = zdt.withZoneSameInstant(id);
        id = ZoneId.of("Pacific/Honolulu");
        ZonedDateTime honoluluMasDos = zdt.withZoneSameInstant(id).plusHours(2);
        for (String zone:ZoneId.getAvailableZoneIds()){
            System.out.println("zone: "+zone);
        }
        
        
    }
    
}
