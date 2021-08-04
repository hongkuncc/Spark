package constant;

/**
 * @ClassName: CalculateMode
 * @Description: 跳过宽表数据准备，直接取宽表数据，全量数据一起计算
 * @Author: hongkuncc
 * @Date 2021/8/4 0:17
 * @Version 1.0
 */
public enum CalculateMode {
    PARTONE("partone",1),PARTTWO("parttwo",2),RMENUMERATION("rmenumeration",3),ALL("all",4);

    private String name;
    private int index;

    //构造方法
    private CalculateMode(String name, int index) {
        this.name = name;
        this.index = index;
    }

    //普通方法
    public static String getName(int index) {
        for (CalculateMode mode:CalculateMode.values()
             ) {
            if (mode.getIndex() == index){
                return  mode.name;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "CalculateMode{" +
                "name='" + name + '\'' +
                ", index=" + index +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
