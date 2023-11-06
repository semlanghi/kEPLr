package evaluation.esper;

import evaluation.keplr.W1;

public class ParametricQueries {

    private long within;

    public ParametricQueries(long within) {
        this.within = within+1;
    }

    public long getWithin() {
        return within;
    }

    public String getW1(){
        return "@name('prova')select * from pattern [every x=A -> ((every y=B) where timer:within (" + this.within + " milliseconds))]; ";
    }

    public String getW2(){
        return "@name('prova')select * from pattern [every x=A -> y=B where timer:within (" + this.within + "milliseconds)];";
    }

    public String getW3(){
        return "@name('prova')select * from pattern [every (x=A -> y=B where timer:within (" + this.within + "milliseconds))];";
    }

    public String getW4(){
        return "@name('prova')select * from pattern [x=A -> (every y=B)]; ";
    }

}
