package org.example.state;

public class Ballot {
    private long number;
    private int serverId;

    public Ballot(long number, int serverId) {
        this.number = number;
        this.serverId = serverId;
    }

    public Ballot(org.example.Ballot protoBallot) {
        this.number = protoBallot.getInstance();
        this.serverId = protoBallot.getSenderId();
    }

    public long getNumber() {
        return number;
    }

    public int getServerId() {
        return serverId;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public void setBallot(long number, int serverId) {
        this.number = number;
        this.serverId = serverId;
    }

    public void setBallot(Ballot other) {
        this.number = other.number;
        this.serverId = other.serverId;
    }

    public void incrementBallot(int serverId) {
        this.number += 1;
        this.serverId = serverId;
    }

    public boolean isGreaterThan(Ballot other) {
        if (this.number > other.number) {
            return true;
        } else if (this.number == other.number) {
            return this.serverId > other.serverId;
        } else {
            return false;
        }
    }

    public boolean isGreaterThan(org.example.Ballot other) {
        return isGreaterThan(new Ballot(other));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Ballot ballot = (Ballot) obj;
        return number == ballot.number && serverId == ballot.serverId;
    }

    @Override
    public String toString() {
        return "Ballot<" +
                + number +
                ", " + serverId +
                '>';
    }
}
