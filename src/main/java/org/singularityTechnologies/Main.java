package org.singularityTechnologies;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
          ElectricVehicle electricVehicle = new  ElectricVehicle();
        electricVehicle.getTopThreeUsedVehiclesSQL();
        electricVehicle.getTopThreeUsedVehiclesDS();

    }
}