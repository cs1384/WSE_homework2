package edu.nyu.cs.cs2580;

import java.util.ArrayList;

public class VByteEncoder
{

    public VByteEncoder()
    {
    }
    
    //public static ArrayList<Byte> encode(ArrayList<Integer> nums)
    public static byte[] encode(ArrayList<Integer> nums)
    {
        //ArrayList<Byte> byteStream = new ArrayList<Byte>();
        int totalSize = 0;
        for(int i=0;i<nums.size();i++)
        {
            int n = nums.get(i);
            int sizeReq = 1;
            if(n < Math.pow(2, 7))
                sizeReq = 1;
            else if(n < Math.pow(2, 14))
                sizeReq = 2;
            else if(n < Math.pow(2, 21))
                sizeReq = 3;
            else if(n < Math.pow(2, 28))
                sizeReq = 4;
            totalSize += sizeReq;
        }
        byte totalArray[] = new byte[totalSize];
        int c = 0;
        for(int i=0;i<nums.size();i++)
        {
            byte array[] = encode(nums.get(i));
            //byteStream.addAll(array);
            for(int j=0;j<array.length;j++)
            totalArray[c++] = array[j];
        }
        return totalArray;
    }
    
    public static byte[] encode(int n)
    {
        int sizeReq = 1;
        if(n < Math.pow(2, 7))
            sizeReq = 1;
        else if(n < Math.pow(2, 14))
            sizeReq = 2;
        else if(n < Math.pow(2, 21))
            sizeReq = 3;
        else if(n < Math.pow(2, 28))
            sizeReq = 4;
            
        //ArrayList<Byte> array = new ArrayList<Byte>();
        byte array[] = new byte[sizeReq];
        int i = sizeReq-1;
        while(true)
        {
            int x = n % 128;
            array[i] = (byte)x;
            if(n < 128)
                break;
            
            n = n / 128;
            i--;
        }
        byte newLast = (byte) ((byte)array[sizeReq-1] | (byte)(1 << 7));
        array[sizeReq-1] = (byte)newLast;
        
        return array;
        
    }
    
    public static ArrayList<Integer> decode(ArrayList<Byte> list)
    {
        byte array[] = new byte[list.size()];
        for(int i=0;i<list.size();i++)
            array[i] = list.get(i);
        
        return decode(array);
    }
    public static ArrayList<Integer> decode(byte array[])
    {
        int n = 0;
        ArrayList<Integer> intArray = new ArrayList<Integer>();
        for(int i=0;i<array.length;i++)
        {
            byte b = array[i];
            boolean last = ((b & (byte)(1 << 7)) != 0);
            if(!last)
                n = 128 * n + (int)b;
            else
            {
                n = 128 * n + (int) (b & (byte)127);
                intArray.add(n);
                //System.out.println(n);
                n = 0;
            }
        }
        return intArray;
    }
    
    public static void main(String[] args)
    {

        //convert(300);
        ArrayList<Integer> nums = new ArrayList<Integer>();
        nums.add(1);
        nums.add(2);
        nums.add(10000);
        nums.add(200);
        
        
        byte array[] = VByteEncoder.encode(nums);
        for(int i=0;i<array.length;i++)
        {
            
            System.out.println(Integer.toHexString(array[i]));
        }
        
        
        ArrayList<Integer> nums2 = decode(array);
        for(int i=0;i<nums2.size();i++)
        {
            System.out.println(nums2.get(i));
        }
        for(int i=0;i<array.length;i++)
        {
            byte b1 = array[i];
            String s1 = String.format("%8s", Integer.toBinaryString(b1 & 0xFF)).replace(' ', '0');
            System.out.println(s1); // 10000001
            if((b1 & (byte)(1 << 7)) != 0)
                System.out.println("last");
        }
        
    }
}
